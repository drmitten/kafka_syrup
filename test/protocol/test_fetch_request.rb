# encoding: ASCII-8BIT

require 'minitest_helper'

class TestFetchRequest < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::FetchRequest.new
  end

  def topic
    @topic ||= KafkaSyrup::Protocol::FetchRequest::Topic.new(:foo, [])
  end

  def test_api_key
    assert_equal 1, obj.api_key
  end

  def test_that_add_topic_returns_the_added_topic
    assert_empty obj.topics
    topic = obj.add_topic(:foo)
    assert_kind_of KafkaSyrup::Protocol::FetchRequest::Topic, topic
    assert_includes obj.topics, topic
  end

  def test_that_add_partition_returns_the_added_partition
    assert_empty topic.partitions
    partition = topic.add_partition(1, 13)
    assert_kind_of KafkaSyrup::Protocol::FetchRequest::Partition, partition
    assert_includes topic.partitions, partition
  end

  def test_it_encodes_correctly
    obj.add_topic(:foo).add_partition(3, 13)

    expected = [
      "\xFF\xFF\xFF\xFF",                     # ReplicaID (-1)
      "\x00\x00\x00\x64",                     # MaxWaitTime (100)
      "\x00\x00\x00\x01",                     # MinBytes (1)
      "\x00\x00\x00\x01",                     # Number of topics (1)
        "\x00\x03foo",                        # Topic name (foo)
        "\x00\x00\x00\x01",                   # Number of partitions (1)
          "\x00\x00\x00\x03",                 # Partition ID (3)
          "\x00\x00\x00\x00\x00\x00\x00\x0D", # Offset (13)
          "\x00\x10\x00\x00"                  # MaxBytes (1MB)
    ].join

    assert_equal expected, obj.encode[14..-1]
  end
end
