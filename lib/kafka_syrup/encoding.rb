module KafkaSyrup
  module Encoding
    class << self
      # Integer encoding methods
      { 8 => 'c',
        16 => 's>',
        32 => 'l>',
        64 => 'q>'
      }.each do |size, pattern|
        define_method "write_int#{size}" do |num|
          [num].pack(pattern)
        end

        define_method "read_int#{size}" do |io|
          io.read(size/8).unpack(pattern).first
        end
      end

      # String and Byte encoding methods
      { string: 16,
        bytes: 32
      }.each do |type, size|
        define_method "write_#{type}" do |val|
          len = val.to_s.length
          if len > 0
            send("write_int#{size}", len) + val.to_s
          else
            send("write_int#{size}", -1)
          end
        end

        define_method "read_#{type}" do |io|
          len = send("read_int#{size}", io)
          if len > 0
            io.read(len)
          end
        end
      end

      def write_array(items, &block)
        result  =  write_int32(items.length)
        if block_given?
          result += items.map(&block).join
        else
          items.each do |item|
            result += item.respond_to?(:encode) ? item.encode : item
          end
        end
        result
      end

      def read_array(io, &block)
        [].tap do |result|
          read_int32(io).times{ result << yield(io) }
        end
      end
    end
  end
end
