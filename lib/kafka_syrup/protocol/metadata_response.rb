module KafkaSyrup
  module Protocol
    class MetadataResponse < Response
      attr_accessor :brokers, :topics

      def defaults
        { brokers: [], topics: [] }
      end

      def add_broker(node, host, port)
        broker = Broker.new(node, host, port)
        brokers << broker
        broker
      end

      def add_topic(code, name)
        topic = Topic.new(code, name, [])
        topics << topic
        topic
      end

      def encode
        super do
          [
            E.write_array(brokers),
            E.write_array(topics)
          ].join
        end
      end

      def decode(io)
        super
        self.brokers = E.read_array(io, &Broker.method(:decode))
        self.topics = E.read_array(io, &Topic.method(:decode))

        topics.map(&:code).each(&KafkaResponseErrors.method(:raise_from_code))
        topics.flat_map(&:partitions).map(&:code).each(&KafkaResponseErrors.method(:raise_from_code))
      end

      Broker = Struct.new(:node, :host, :port) do
        def encode
          [
            E.write_int32(node),
            E.write_string(host),
            E.write_int32(port)
          ].join
        end

        def self.decode(io)
          new(
            E.read_int32(io),   # Node
            E.read_string(io),  # Host
            E.read_int32(io)    # Port
          )
        end
      end

      Topic = Struct.new(:code, :name, :partitions) do
        def add_partition(p_code, id, leader, replicas, isr)
          partition = Partition.new(p_code, id, leader, replicas, isr)
          partitions << partition
          partition
        end

        def encode
          [
            E.write_int16(code),
            E.write_string(name),
            E.write_array(partitions)
          ].join
        end

        def self.decode(io)
          new(
            E.read_int16(io),                            # Error Code
            E.read_string(io),                           # Name
            E.read_array(io, &Partition.method(:decode)) # Partitions
          )
        end
      end

      Partition = Struct.new(:code, :id, :leader, :replicas, :isr) do
        def encode
          [
            E.write_int16(code),
            E.write_int32(id),
            E.write_int32(leader),
            E.write_array(replicas, &E.method(:write_int32)),
            E.write_array(isr, &E.method(:write_int32))
          ].join
        end

        def self.decode(io)
          new(
            E.read_int16(io),                         # Error Code
            E.read_int32(io),                         # ID
            E.read_int32(io),                         # Leader
            E.read_array(io, &E.method(:read_int32)), # Replicas
            E.read_array(io, &E.method(:read_int32))  # ISR
          )
        end
      end
    end
  end
end
