module KafkaSyrup
  module Protocol
    class Base
      include Utils

      def initialize(*args, &block)
        if(io = args.first).respond_to?(:read)
          decode(io, &block)
        else
          load_args(defaults)
          load_args(*args)
        end
      end

      def defaults
        {}
      end

      def config
        @config ||= ::KafkaSyrup.config
      end

      def encode(&block)
        encoded = block_given? ? yield : ""

        [
          E.write_int32(encoded.length),
          encoded
        ].join
      end

      def decode(io)
        E.read_int32(io) # Total length (ignored)
      end

      def ==(obj)
        obj.encode.bytes == encode.bytes
      end
    end
  end
end
