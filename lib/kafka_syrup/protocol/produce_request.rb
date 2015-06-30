module KafkaSyrup
  module Protocol
    class ProduceRequest < Request
      self.api_key = 0

      attr_accessor :required_acks, :timeout, :topics

      def defaults
        {
          required_acks: config.produce_required_acks,
          timeout: config.produce_timeout,
          topics: []
        }
      end

      def add_topic(name)
        topic = Topic.new(name, [])
        topics << topic
        topic
      end

      def encode
        super do
          [
            E.write_int16(required_acks),
            E.write_int32(timeout),
            E.write_array(topics)
          ].join
        end
      end

      Topic = Struct.new(:name, :partitions) do
        def add_partition(id)
          partition = Partition.new(id, MessageSet.new)
          partitions << partition
          partition
        end

        def encode
          [
            E.write_string(name),
            E.write_array(partitions)
          ].join
        end
      end

      Partition = Struct.new(:id, :message_set) do
        def messages
          message_set.messages
        end

        def add_message(value = nil, opts = {})
          msg = Message.new(opts.merge(value: value))
          message_set.messages << msg
          msg
        end

        def encode
          encoded = message_set.encode
          [
            E.write_int32(id),
            E.write_int32(encoded.length),
            encoded
          ].join
        end
      end
    end
  end
end
