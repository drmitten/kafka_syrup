# encoding: ASCII-8BIT

require 'minitest_helper'

class TestMessageSet < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::MessageSet.new
  end

  def message_foo
    @message_foo ||= KafkaSyrup::Protocol::Message.new(value: 'foo', offset: 13)
  end

  def message_bar
    @message_bar ||= KafkaSyrup::Protocol::Message.new(value: 'bar')
  end

  def setup
    obj.messages << message_foo
    obj.messages << message_bar
  end

  def test_it_encodes_correctly
    expected = [
      "\x00\x00\x00\x00\x00\x00\x00\x0D", # Offset of first message
      "\x00\x00\x00\x11",                 # Length of first message
      message_foo.encode,                 # First message
      "\x00\x00\x00\x00\x00\x00\x00\x00", # Offset of second message
      "\x00\x00\x00\x11",                 # Length of second message
      message_bar.encode,                 # Second message
    ].join

    assert_equal expected, obj.encode
  end

  def test_that_it_decodes_correctly
    assert_equal obj, KafkaSyrup::Protocol::MessageSet.new(StringIO.new(obj.encode), 58)
  end

  def test_that_it_yields_for_each_message
    test_messages = []
    KafkaSyrup::Protocol::MessageSet.new(StringIO.new(obj.encode), 58) do |msg|
      test_messages << msg
    end
    assert_includes test_messages, message_foo
    assert_includes test_messages, message_bar
  end

  def test_that_it_ignores_partial_messages
    test_messages = []
    set = KafkaSyrup::Protocol::MessageSet.new(StringIO.new(obj.encode[0..-5]), 53) do |msg|
      test_messages << msg
    end
    assert_includes set.messages, message_foo
    assert_includes test_messages, message_foo
    refute_includes set.messages, message_bar
    refute_includes test_messages, message_bar
  end
end
