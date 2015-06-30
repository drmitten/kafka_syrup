# encoding: ASCII-8BIT

require 'minitest_helper'

class TestRequest < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::Request.new(correlation_id: 17, client_id: :foo)
  end

  def test_it_does_not_have_api_key
    assert_nil obj.api_key
  end

  def test_it_encodes_correctly
    obj.stubs(api_key: 200)
    assert_equal "\x00\x00\x00\x0D\x00\xC8\x00\x00\x00\x00\x00\x11\x00\x03foo", obj.encode
  end
end
