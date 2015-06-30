module KafkaSyrup
  module Protocol
    class MessageSet
      include Utils

      attr_accessor :messages

      def initialize(*args)
        io, total_length, *_ = args
        if io.respond_to?(:read)
          total_length = 0 unless total_length.is_a?(Fixnum)

          read_length = 0

          self.messages = []

          while read_length < total_length && !io.eof?
            offset = E.read_int64(io)
            read_length += 8
            msg_length = E.read_int32(io)
            read_length += 4
            msg = Message.new(io, offset: offset, length: msg_length) rescue nil
            messages << msg
            read_length += msg_length
            yield(msg) if block_given? && msg
          end
        else
          load_args(defaults)
          load_args(*args)
        end
      end

      def defaults
        { messages: [] }
      end

      def encode
        messages.map{ |msg|
          encoded = msg.encode
          [
            E.write_int64(msg.offset.to_i),
            E.write_int32(encoded.length),
            encoded
          ].join
        }.join
      end

      def ==(obj)
        obj.encode == encode
      end
    end
  end
end
