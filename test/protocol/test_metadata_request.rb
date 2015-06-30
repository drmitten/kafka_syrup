# encoding: ASCII-8BIT

require 'minitest_helper'

class TestMetadataRequest < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::MetadataRequest.new
  end

  def test_api_key
    assert_equal 3, obj.api_key
  end

  def test_it_encodes_correctly
    obj.topics += %w[foo bar]
    assert_equal "\x00\x00\x00\x02\x00\x03foo\x00\x03bar", obj.encode[14..-1]
  end

  def test_it_accepts_topics_when_initializing
    req = KafkaSyrup::Protocol::MetadataRequest.new(:foo, :bar)
    assert_equal "\x00\x00\x00\x02\x00\x03foo\x00\x03bar", req.encode[14..-1]
  end
end
