# encoding: ASCII-8BIT

require 'minitest_helper'

class TestMessage < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::Message.new(key: :foo, value: :bar)
  end

  def test_it_has_magic_byte
    assert_equal 0, KafkaSyrup::Protocol::Message::MAGIC_BYTE
  end

  def test_it_encodes_correctly
    expected = [
      "\xB8\xBA\x5F\x57",    #CRC6
      "\x00",                # MagicByte of 0
      "\x00",                # No Compression
      "\x00\x00\x00\x03foo", # Key
      "\x00\x00\x00\x03bar", # Value
    ].join
    assert_equal expected, obj.encode
  end

  def test_that_it_decodes_correctly
    assert_equal obj, KafkaSyrup::Protocol::Message.new(StringIO.new(obj.encode))
  end
end
