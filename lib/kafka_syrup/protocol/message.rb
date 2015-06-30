require 'zlib'

module KafkaSyrup
  module Protocol
    class Message
      include Utils

      MAGIC_BYTE = 0

      attr_accessor :key, :value, :offset

      def initialize(*args)
        if (io = args.first).respond_to?(:read)
          opts = { check_crc: false }
          opts.merge(args.last) if args.last.is_a?(Hash)
          load_args(opts)

          crc = E.read_int32(io)
          E.read_int8(io)     # Magic Byte (ignored)
          E.read_int8(io)     # Compression (ignored)
          self.key = E.read_bytes(io)
          self.value = E.read_bytes(io)
          # TODO Verify CRC
        end

        load_args(*args)
      end

      def encode
        [
          E.write_int32(crc),
          encoded
        ].join
      end

      def encoded
        @encoded ||= [
          E.write_int8(MAGIC_BYTE),
          E.write_int8(0), # Currently don't support compression
          E.write_bytes(key),
          E.write_bytes(value)
        ].join
      end

      def crc
        @crc ||= Zlib.crc32(encoded)
      end

      def key
        @key.to_s.empty? ? nil : @key
      end

      def ==(obj)
        obj.encode == encode
      end
    end
  end
end
