module KafkaSyrup
  module Protocol
    class FetchResponse < Response
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

      def decode(io, &block)
        super
        self.topics = E.read_array(io){ |input| Topic.decode(input, &block) }
        topics.flat_map(&:partitions).map(&:code).each(&KafkaResponseErrors.method(:raise_from_code))
      end

      Topic = Struct.new(:name, :partitions) do
        def add_partition(id, code, highwater_offset)
          partition = Partition.new(id, code, highwater_offset, MessageSet.new)
          partitions << partition
          partition
        end

        def encode
          [
            E.write_string(name),
            E.write_array(partitions)
          ].join
        end

        def self.decode(io, &block)
          new(
            E.read_string(io),                                          # Name
            E.read_array(io){ |input| Partition.decode(input, &block) } # Partitions
          )
        end
      end

      Partition = Struct.new(:id, :code, :highwater_offset, :message_set) do
        def messages
          message_set.messages
        end

        def add_message(value = nil, opts = {})
          m = Message.new(opts.merge(value: value))
          message_set.messages << m
          m
        end

        def encode
          encoded = message_set.encode
          [
            E.write_int32(id),
            E.write_int16(code),
            E.write_int64(highwater_offset),
            E.write_int32(encoded.length),
            encoded
          ].join
        end

        def self.decode(io, &block)
          partition = new
          partition.id = E.read_int32(io)
          partition.code = E.read_int16(io)
          partition.highwater_offset = E.read_int64(io)
          length = E.read_int32(io)
          partition.message_set = MessageSet.new(io, length, &block)
          partition
        end
      end
    end
  end
end
