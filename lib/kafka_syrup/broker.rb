module KafkaSyrup
  class Broker
    attr_accessor :host, :port

    def initialize(host, port)
      self.host = host
      self.port = port
      self.extend Communications
    end

    module Communications
      def socket
        unless @socket.respond_to?(:closed?) && !@socket.closed?
          @socket = Socket.new(:INET, :SOCK_STREAM)
          @socket.setsockopt(:SOCKET, :SO_SNDBUF, KafkaSyrup.config.so_sndbuf)
          @socket.connect(Socket.pack_sockaddr_in(port, host))
        end
        @socket
      rescue => e
        @socket.close rescue
        @socket = nil
        raise e
      end

      def send_request(req, opts = {}, &block)
        begin
          socket.write(req.encode)

          response = self.class.const_get(req.class.to_s.sub(/Request$/, 'Response')).new(socket, &block)
        rescue KafkaSyrup::KafkaResponseError => e
          raise e
        rescue StandardError => e
          raise KafkaSyrup::SocketReadError.new(e)
        end

        socket.close if opts[:close]

        response
      end
    end
  end
end
