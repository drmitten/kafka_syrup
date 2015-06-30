module KafkaSyrup
  module Protocol
    class MetadataRequest < Request
      self.api_key = 3

      attr_accessor :topics

      def initialize(*args)
        opts = args.last.is_a?(Hash) ? args.pop : {}

        load_args(opts)

        self.topics = args
      end

      def encode
        super do
          E.write_array(topics, &E.method(:write_string))
        end
      end
    end
  end
end
