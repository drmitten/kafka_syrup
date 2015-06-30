# encoding: ASCII-8BIT

require 'minitest_helper'

class TestProduceRequest < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::ProduceRequest.new
  end

  def topic
    @topic ||= KafkaSyrup::Protocol::ProduceRequest::Topic.new(:foo, [])
  end

  def partition
    @partition ||= KafkaSyrup::Protocol::ProduceRequest::Partition.new(0, KafkaSyrup::Protocol::MessageSet.new)
  end

  def test_api_key
    assert_equal 0, obj.api_key
  end

  def test_it_has_defaults
    assert_equal(-1, obj.required_acks)
  end

  def test_that_add_topic_returns_the_added_topic
    assert_empty obj.topics
    topic = obj.add_topic('foo')
    assert_kind_of KafkaSyrup::Protocol::ProduceRequest::Topic, topic
    assert_includes obj.topics, topic
  end

  def test_that_add_partition_returns_the_added_partition
    assert_empty topic.partitions
    partition = topic.add_partition(1)
    assert_kind_of KafkaSyrup::Protocol::ProduceRequest::Partition, partition
    assert_includes topic.partitions, partition
  end

  def test_that_add_message_returns_the_added_message
    assert_empty partition.messages
    msg = partition.add_message(value: :foo)
    assert_kind_of KafkaSyrup::Protocol::Message, msg
    assert_includes partition.messages, msg
  end

  def test_it_encodes_correctly
    obj.add_topic(:foo).add_partition(1).add_message(:bar)
    expected = [
      "\xFF\xFF", # Required Acks
      "\x00\x00\x05\xDC", # Timeout
      "\x00\x00\x00\x01", # Topics array length
        "\x00\x03foo", # First topic name
        "\x00\x00\x00\x01", # First topic's partitions array length
          "\x00\x00\x00\x01", # First topic's first partition id
          "\x00\x00\x00\x1D", # First topic's first partition's message_set length
          "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x11\x00\a\xF2\xC7\x00\x00\xFF\xFF\xFF\xFF\x00\x00\x00\x03bar" # First topic's first parition's message_set
    ].join

    assert_equal expected, obj.encode[14..-1]
  end
end
