# encoding: ASCII-8BIT

require 'minitest_helper'

class TestResponse < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::Response.new(correlation_id: 13)
  end

  def test_that_it_encodes_correctly
    assert_equal "\x00\x00\x00\x04\x00\x00\x00\x0D", obj.encode
  end

  def test_that_it_decodes_correctly
    assert_equal KafkaSyrup::Protocol::Response.new(StringIO.new(obj.encode)), obj
  end
end
