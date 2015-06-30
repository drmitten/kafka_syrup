# encoding: ASCII-8BIT

require 'minitest_helper'

class TestFetchResponse < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::FetchResponse.new
  end

  def test_that_add_topic_returns_the_added_topic
    assert_empty obj.topics
    t = obj.add_topic(:foo)
    assert_kind_of KafkaSyrup::Protocol::FetchResponse::Topic, t
    assert_includes obj.topics, t
  end

  def test_that_add_partition_returns_the_added_partition
    topic = obj.add_topic(:foo)
    assert_empty topic.partitions
    p = topic.add_partition(1, 0, 50)
    assert_kind_of KafkaSyrup::Protocol::FetchResponse::Partition, p
    assert_includes topic.partitions, p
  end

  def test_that_add_message_returns_the_added_message
    partition = obj.add_topic(:foo).add_partition(1, 0, 50)
    assert_empty partition.messages
    m = partition.add_message(:foo)
    assert_kind_of KafkaSyrup::Protocol::Message, m
    assert_includes partition.messages, m
  end

  def test_that_it_encodes_correctly
    obj.add_topic(:foo).add_partition(1, 0, 50).add_message(:bar, key: :baz, offset: 42)
    expected = [
      "\x00\x00\x00\x01",                                                                                                                 # Number of topics(1)
        "\x00\x03foo",                                                                                                                    # Topic name(foo)
        "\x00\x00\x00\x01",                                                                                                               # Number of partitions(1)
          "\x00\x00\x00\x01",                                                                                                             # Partition ID(1)
          "\x00\x00",                                                                                                                     # Error code(0)
          "\x00\x00\x00\x00\x00\x00\x00\x32",                                                                                             # Highwater Offset(50)
          "\x00\x00\x00\x20",                                                                                                             # Size of message_set(36)
          "\x00\x00\x00\x00\x00\x00\x00\x2A\x00\x00\x00\x14\x4D\xC8\e\xF0\x00\x00\x00\x00\x00\x03baz\x00\x00\x00\x03bar"  # Message set
    ].join

    assert_equal expected, obj.encode[8..-1]
  end

  def test_that_it_decodes_correctly
    obj.add_topic(:foo).tap do |topic|
      topic.add_partition(0, 0, 50).add_message(:bar, key: :baz, offset: 42)
      topic.add_partition(1, 0, 50).add_message(:howdy, key: :partner, offset: 43)
    end
    assert_equal obj, KafkaSyrup::Protocol::FetchResponse.new(StringIO.new(obj.encode))
  end

  def test_that_it_processes_the_messages_as_they_come_in
    obj.add_topic(:foo).add_partition(1, 0, 50).add_message(:bar, key: :baz, offset: 42)
    test_messages = []
    KafkaSyrup::Protocol::FetchResponse.new(StringIO.new(obj.encode)) do |msg|
      test_messages << msg
    end
    assert_equal 1, test_messages.count
  end

  def test_that_it_raises_an_exception_when_error_code_not_0
    obj.add_topic(:foo).add_partition(1, 4, 0)
    assert_raises(KafkaSyrup::KafkaResponseErrors::InvalidMessageSize) do
      KafkaSyrup::Protocol::FetchResponse.new(StringIO.new(obj.encode))
    end
  end
end
