module KafkaSyrup
  class TopicProducer
    include Utils

    attr_accessor :topic, :brokers, :partitions, :partitioner, :opts

    def initialize(topic, *args)
      self.topic = topic
      self.opts = args.last || {}

      self.brokers = {}
      self.partitions = {}

      refresh_metadata

      self.partitioner ||= ->(msg) do
        msg.hash % partitions.count
      end
    end

    def refresh_metadata
      brokers.each{ |id, broker| broker && broker.socket && broker.socket.close }

      self.brokers.clear
      self.partitions.clear

      meta = KafkaSyrup.get_metadata(topic)

      meta.brokers.each do |broker|
        broker.extend KafkaSyrup::Broker::Communications
        brokers.store broker.node, broker
      end

      meta.topics.first.partitions.each do |partition|
        partitions.store partition.id, partition
      end
    end

    def send_message(msg)
      id = partitioner.call(msg)

      request = KafkaSyrup::Protocol::ProduceRequest.new
      request.add_topic(topic).add_partition(id).add_message(msg)

      broker_for_id(id).send_request(request)

    rescue KafkaResponseErrors::NotLeaderForPartition, SocketReadError
      refresh_metadata

      retry
    end

    private

    def broker_for_id(id)
      brokers[partitions[id].leader]
    end
  end
end
