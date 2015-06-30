require 'minitest_helper'

class TestBroker < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Broker.new('localhost', 13)
  end

  def request
    @request ||= KafkaSyrup::Protocol::MetadataRequest.new
  end

  def response
    @response ||= KafkaSyrup::Protocol::MetadataResponse.new
  end

  class TestSocketMethod < TestBroker
    def setup
      Socket.any_instance.stubs(connect: true)
    end

    def test_it_returns_a_connected_socket
      refute obj.socket.closed?
    end

    def test_it_returns_a_new_socket_if_current_is_closed
      obj.socket
      obj.socket.stubs(closed?: true)
      refute obj.socket.closed?
    end
  end

  class TestSendRequestMethod < TestBroker
    def setup
      obj.stubs(socket: StringIO.new(response.encode))
      obj.socket.stubs(write: nil)
    end

    def test_it_write_encoded_message_to_socket
      obj.socket.expects(:write).with(request.encode)
      obj.send_request(request)
    end

    def test_it_returns_appropriate_response_type
      assert_equal response, obj.send_request(request)
    end

    def test_it_raises_branded_exception
      obj.socket.close_read
      assert_raises(KafkaSyrup::SocketReadError) { obj.send_request(request) }
    end

    def test_it_honors_the_close_option
      refute obj.socket.closed?
      obj.send_request(request, close: true)
      assert obj.socket.closed?
    end
  end
end
