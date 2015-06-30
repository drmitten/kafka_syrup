module KafkaSyrup
  class PartitionConsumer
    include Utils

    attr_accessor :topic, :partition, :broker, :offset, :max_bytes, :messages, :thread, :control_queue, :lock

    def initialize(*args)
      load_args(*args)

      refresh_metadata

      self.messages = Queue.new
      self.control_queue = Queue.new
      self.lock = Mutex.new
    end

    def refresh_metadata
      broker && broker.socket && broker.socket.close

      meta = KafkaSyrup.get_metadata(topic)

      self.broker = meta.brokers.detect{ |b| b.node == partition_from_response(meta).leader }
      raise BrokerNotFound unless self.broker
      self.broker.extend KafkaSyrup::Broker::Communications
    end

    def get_available_offset(time = :latest)
      request = KafkaSyrup::Protocol::OffsetRequest.new
      request.add_topic(topic).add_partition(partition, time == :earliest ? -2 : -1)

      response = broker.send_request(request)
      partition_from_response(response).offsets.last
    end

    def fetch_from_broker(&block)
      lock.synchronize{ self.offset = get_available_offset(offset) } unless offset.is_a?(Fixnum)

      opts = { max_bytes: max_bytes } if max_bytes
      request = KafkaSyrup::Protocol::FetchRequest.new(opts)
      request.add_topic(topic).add_partition(partition, offset)

      response = partition_from_response(broker.send_request(request, &block))

      lock.synchronize{ self.offset = response.messages.last.offset + 1 } unless response.messages.empty?
    rescue KafkaSyrup::KafkaResponseErrors::OffsetOutOfRange
      low = get_available_offset(:earliest)
      high = get_available_offset

      lock.synchronize{ self.offset = offset < low ? low : high }
    end

    def fetch(limit = nil)
      start_fetcher_thread unless thread

      control_queue.push(:fetch) if messages.empty? && control_queue.num_waiting > 0

      result = []

      loop do
        result << messages.pop
        break if messages.empty? || (limit && result.count == limit)
      end

      self.offset = result.last[:offset] + 1

      result
    end

    def retry_backoff
      @retry_backoff ||= KafkaSyrup.config.retry_backoff / 1000.0
    end

    private

    def partition_from_response(response)
      topic_meta = response.topics.detect{ |t| t.name == topic.to_s }
      raise TopicNotFound unless topic_meta
      partition_meta = topic_meta.partitions.detect{ |p| p.id == partition }
      raise PartitionNotFound unless partition_meta
      partition_meta
    end

    def start_fetcher_thread
      self.thread = Thread.new do
        log.debug "Starting Fetcher Thread for partition #{partition}"
        loop do
          begin
            control_queue.pop # wait for start message
            log.debug "Fetching from partition #{partition}"

            num_received = 0
            begin
              fetch_from_broker do |msg|
                messages.push partition: partition, offset: msg.offset, message: msg.value
                num_received += 1
              end

              # No messages received so backoff a bit before retrying
              sleep retry_backoff if num_received == 0
            rescue
              sleep retry_backoff
            end
          rescue
            sleep retry_backoff
          end
        end
      end
      sleep 0.1 # Slight sleep to let the thread start waiting on the control queue (avoids deadlock)
    end
  end
end
