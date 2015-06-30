module KafkaSyrup
  module Protocol
    class OffsetRequest < Request
      self.api_key = 2

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
          [
            E.write_int32(REPLICA_ID),
            E.write_array(topics)
          ].join
        end
      end

      Topic = Struct.new(:name, :partitions) do
        def add_partition(id, time = -1, max_offsets = 1)
          partition = Partition.new(id, time, max_offsets)
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

      Partition = Struct.new(:id, :time, :max_offsets) do
        def encode
          [
            E.write_int32(id),
            E.write_int64(time),
            E.write_int32(max_offsets)
          ].join
        end
      end
    end
  end
end
