module KafkaSyrup
  module Protocol
    class Response < Base

      attr_accessor :correlation_id

      def encode(&block)
        super do
          [
            E.write_int32(correlation_id.to_i),
            block_given? ? yield : ""
          ].join
        end
      end

      def decode(io)
        super(io)
        self.correlation_id = E.read_int32(io)
      end
    end
  end
end
