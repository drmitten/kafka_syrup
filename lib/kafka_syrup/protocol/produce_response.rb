module KafkaSyrup
  module Protocol
    class ProduceResponse < Response
      attr_accessor :topics

      def defaults
        { topics: [] }
      end

      def add_topic(name)
        topic = Topic.new(name, [])
        topics << topic
        topic
      end

      def encode
        super do
          E.write_array(topics)
        end
      end

      def decode(io)
        super
        self.topics = E.read_array(io, &Topic.method(:decode))
        topics.flat_map(&:partitions).map(&:code).each(&KafkaResponseErrors.method(:raise_from_code))
      end

      Topic = Struct.new(:name, :partitions) do
        def add_partition(id, code, offset)
          partition = Partition.new(id, code, offset)
          partitions << partition
          partition
        end

        def encode
          [
            E.write_string(name),
            E.write_array(partitions)
          ].join
        end

        def self.decode(io)
          new(
            E.read_string(io),                            # Name
            E.read_array(io, &Partition.method(:decode))  # Partitions
          )
        end
      end

      Partition = Struct.new(:id, :code, :offset) do
        def encode
          [
            E.write_int32(id),
            E.write_int16(code),
            E.write_int64(offset)
          ].join
        end

        def self.decode(io)
          new(
            E.read_int32(io), # ID
            E.read_int16(io), # Code
            E.read_int64(io)  # Offset
          )
        end
      end
    end
  end
end
