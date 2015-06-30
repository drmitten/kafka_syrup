require 'zk'

module KafkaSyrup
  class TopicConsumer
    include Utils

    attr_accessor :topic, :group, :consumer_id, :watcher, :partitions, :offsets, :lock, :threads, :messages, :control_queues, :offset_mode, :max_bytes, :commit_mode, :uncommitted_partitions

    def initialize(*args)
      load_args(defaults)
      load_args(*args)
    end

    def defaults
      {
        consumer_id: "#{Socket.gethostname}-#{hash}",
        partitions: [],
        offsets: {},
        lock: Mutex.new,
        threads: [],
        messages: Queue.new,
        control_queues: [],
        offset_mode: :latest,
        commit_mode: :fetch,
        uncommitted_partitions: []
      }
    end

    def zk
      if @zk.is_a?(ZK::Client::Threaded) && @zk.connected?
        @zk
      else
        @zk = ZK.new(KafkaSyrup.config.zookeeper_hosts, chroot: :check)
      end
    end

    def register
      return if registered?

      # Make sure nodes exist in Zookeeper
      zk.mkdir_p(membership_path)
      zk.mkdir_p(ownership_path)
      zk.mkdir_p(offsets_path)

      begin
        zk.create(registration_path, MultiJson.dump(new_registration_info), ephemeral: true)

      rescue ZK::Exceptions::NodeExists
        no_node = false
        begin
          zk.set(registration_path, MultiJson.dump(new_registration_info))
        rescue ::ZK::Exceptions::NoNode
          no_node = true
        end
        retry if no_node
      ensure
        self.watcher = zk.register(membership_path) do |event|
          rebalance
          zk.children(membership_path, watch: true)
          trigger_fetch
        end
        zk.children(membership_path, watch: true)
        rebalance
        registered?
      end
    end

    def unregister
      watcher.unregister
      threads.each(&:kill)
      [threads, messages, control_queues].each(&:clear)
      release_partitions
      zk.rm_rf([membership_path, consumer_id]*'/')
    end

    def rebalance
      raise NotRegistered unless registered?

      log.debug "Rebalance triggered on group #{group}"

      # Make sure fetch doesn't attempt to store offsets during a rebalance
      lock.synchronize do
        begin
          # Stop fetcher threads and clear locally cached messages
          threads.each(&:kill)
          [threads, messages, control_queues].each(&:clear)

          # Relinquish client claims to whatever partitions it's currently serving
          release_partitions

          # Determine which partitions to claim
          self.partitions = partition_ids_to_claim

          # Attempt to claim desired partitions in zookeeper
          partitions.each{ |id| zk.create([ownership_path, id]*'/', consumer_id.to_s, ephemeral: true) }

          # Retrieve offsets for successfully claimed partitions
          partitions.each{ |id| offsets[id] = get_offset(id) }

          # Start fetcher threads for partitions
          partitions.each(&method(:start_fetcher_thread))
          sleep 0.01

        rescue ZK::Exceptions::NodeExists
          # It's possible that another consumer has not yet released the partition this client is attempting to claim
          # No biggie - release any partitions this client has already claimed, backoff a bit, and retry
          release_partitions
          sleep 0.2
          retry
        end
      end
    end

    def fetch(limit = nil)
      raise NotRegistered unless registered?

      commit if commit_mode == :fetch

      trigger_fetch if messages.empty?

      results = []

      loop do
        results << messages.pop
        break if messages.empty? || limit && limit == results.count
      end

      # Ensure rebalancing isn't adjusting the offsets
      lock.synchronize do
        results.each do |msg|
          self.offsets[msg[:partition]] = msg[:offset] + 1
          self.uncommitted_partitions |= [msg[:partition]]
        end
      end

      commit if commit_mode == :auto

      results
    end

    def release_partitions
      partitions.each{ |id| zk.rm_rf([ownership_path, id]*'/') }

      partitions.clear
      offsets.clear
    end

    def get_offset(id)
      offset, _ = zk.get([offsets_path, id]*'/')
      offset.to_i
    rescue ZK::Exceptions::NoNode
      nil
    end

    def set_offset(id, offset)
      log.debug "Committing offset #{offset} of partition #{id}"
      zk.set([offsets_path, id]*'/', offset.to_s)
    rescue ZK::Exceptions::NoNode
      node_exists = false
      begin
        zk.create([offsets_path, id]*'/', offset.to_s)
      rescue ZK::Exceptions::NodeExists
        node_exists = true
      end
      retry if node_exists
    end

    def commit
      uncommitted_partitions.each do |id|
        set_offset(id, offsets[id])
      end

      uncommitted_partitions.clear
    end

    def subscribers
      zk.children(membership_path).select do |member|
        info, _ = zk.get([membership_path, member]*'/')

        MultiJson.load(info)['subscription'][topic.to_s].to_i > 0
      end
    end

    def registered?
      info = current_registration_info
      info.is_a?(Hash) && info['subscription'].is_a?(Hash) && info['subscription'][topic.to_s].to_i > 0
    end

    def new_registration_info
      info = current_registration_info || {
        'pattern' => 'static',
        'version' => 1,
        'subscription' => {}
      }

      info['subscription'][topic.to_s] = info['subscription'][topic.to_s].to_i + 1

      info
    end

    def current_registration_info
      info, _ = zk.get(registration_path)

      MultiJson.load(info)
    rescue ZK::Exceptions::NoNode
      nil
    end

    def group_path
      ([KafkaSyrup.config.zookeeper_path, 'consumers', group]*'/').gsub(/\/\/+/, '/')
    end

    def membership_path
      [group_path, 'ids']*'/'
    end

    def ownership_path
      [group_path, 'owners', topic.to_s]*'/'
    end

    def offsets_path
      [group_path, 'offsets', topic.to_s]*'/'
    end

    def registration_path
      [membership_path, consumer_id]*'/'
    end

    private

    def partition_ids_to_claim
      topic_partitions = KafkaSyrup.get_metadata.topics.detect{ |t| t.name == topic.to_s }.partitions

      consumers = subscribers.sort

      partitions_per_consumer = topic_partitions.size / consumers.size

      extra_partitions = topic_partitions.size % consumers.size

      consumer_position = consumers.index(consumer_id.to_s)

      starting_index = consumer_position * partitions_per_consumer + [consumer_position, extra_partitions].min

      num_to_claim = partitions_per_consumer

      num_to_claim += 1 if consumer_position + 1 <= extra_partitions

      topic_partitions.map(&:id).sort.slice(starting_index, num_to_claim)
    end

    def start_fetcher_thread(id)
      # Initialize new control queue
      q = Queue.new

      # Intitialize consumer for this partition
      opts = { topic: topic, partition: id, offset: offsets[id] || offset_mode }
      opts[:max_bytes] = max_bytes if max_bytes
      consumer = PartitionConsumer.new(opts)

      # Fetch on a thread for concurrency
      threads << Thread.new do
        log.debug "Starting Fetcher Thread for partition #{id}"
        loop do
          begin
            q.pop # wait for start message
            log.debug "Fetching from partition #{id}"

            num_received = 0
            while num_received == 0
              begin
                consumer.fetch_from_broker do |msg|
                  messages.push partition: id, offset: msg.offset, message: msg.value
                  num_received += 1
                end

                # No messages received means that the partition has no messages to consume at this time so wait a bit before trying again
                sleep retry_backoff if num_received == 0
              rescue
                sleep retry_backoff
                retry
              end
            end
          rescue
            sleep retry_backoff
          end
        end
      end

      # Store control queue for later use to control the fetcher thread
      control_queues << q
    end

    def retry_backoff
      @retry_backoff ||= KafkaSyrup.config.retry_backoff / 1000.0
    end

    def trigger_fetch
      control_queues.reject{ |q| q.num_waiting == 0 }.each{ |q| q.push(:fetch) }
    end
  end
end
