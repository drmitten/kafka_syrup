module KafkaSyrup
  module Protocol
    class Request < Base

      class << self
        attr_accessor :api_key
      end

      attr_accessor :correlation_id, :client_id

      def initialize(*args)
        load_args(defaults)
        load_args(*args)
      end

      def api_key
        self.class.api_key
      end

      def api_version
        0
      end

      def encode(&block)
        super do
          [
            E.write_int16(api_key),
            E.write_int16(api_version),
            E.write_int32(correlation_id.to_i),
            E.write_string(client_id.to_s),
            block_given? ? yield : ""
          ].join
        end
      end
    end
  end
end
