# encoding: ASCII-8BIT

require 'minitest_helper'

class TestOffsetRequest < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::OffsetRequest.new
  end

  def topic
    @topic ||= KafkaSyrup::Protocol::OffsetRequest::Topic.new(:foo, [])
  end

  def test_api_key
    assert_equal 2, obj.api_key
  end

  def test_that_add_topic_returns_the_added_topic
    assert_empty obj.topics
    topic = obj.add_topic(:foo)
    assert_kind_of KafkaSyrup::Protocol::OffsetRequest::Topic, topic
    assert_includes obj.topics, topic
  end

  def test_that_add_partition_returns_the_added_partition
    assert_empty topic.partitions
    partition = topic.add_partition(3)
    assert_kind_of KafkaSyrup::Protocol::OffsetRequest::Partition, partition
    assert_includes topic.partitions, partition
  end

  def test_it_encodeds_correctly
    obj.add_topic(:foo).add_partition(3, -1, 5)

    expected = [
      "\xFF\xFF\xFF\xFF",                     # Replica ID (-1)
      "\x00\x00\x00\x01",                     # Number of topics (1)
        "\x00\x03foo",                        # Topic name (foo)
        "\x00\x00\x00\x01",                   # Number of paritions (1)
          "\x00\x00\x00\x03",                 # Parition ID (3)
          "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", # Time (-1)
          "\x00\x00\x00\x05"                  # Max offsets (5)
    ].join

    assert_equal expected, obj.encode[14..-1]
  end
end
