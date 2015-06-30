module KafkaSyrup
  module Protocol
    class FetchRequest < Request
      self.api_key = 1

      attr_accessor :max_wait_time, :min_bytes, :max_bytes, :topics

      def defaults
        {
          max_wait_time: config.consume_max_wait_time,
          min_bytes: config.consume_min_bytes,
          max_bytes: config.consume_max_bytes,
          topics: []
        }
      end

      def encode
        super do
          [
            E.write_int32(REPLICA_ID),
            E.write_int32(max_wait_time),
            E.write_int32(min_bytes),
            E.write_array(topics)
          ].join
        end
      end

      def add_topic(name)
        topic = Topic.new(name, [], max_bytes)
        topics << topic
        topic
      end

      Topic = Struct.new(:name, :partitions, :max_bytes) do
        def add_partition(id, offset)
          partition = Partition.new(id, offset, max_bytes)
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

      Partition = Struct.new(:id, :offset, :max_bytes) do
        def encode
          [
            E.write_int32(id),
            E.write_int64(offset),
            E.write_int32(max_bytes)
          ].join
        end
      end
    end
  end
end
