# encoding: ASCII-8BIT

require 'minitest_helper'

class TestMetadataResponse < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::MetadataResponse.new
  end

  def test_that_add_broker_returns_the_added_broker
    assert_empty obj.brokers
    broker = obj.add_broker(1, :hostfoo, 3000)
    assert_kind_of KafkaSyrup::Protocol::MetadataResponse::Broker, broker
    assert_includes obj.brokers, broker
  end

  def test_that_add_topic_returns_the_added_topic
    assert_empty obj.topics
    topic = obj.add_topic(0, :foo)
    assert_kind_of KafkaSyrup::Protocol::MetadataResponse::Topic, topic
    assert_includes obj.topics, topic
  end

  def test_that_add_partition_returns_the_added_partition
    topic = obj.add_topic(0, :foo)
    assert_empty topic.partitions
    partition = topic.add_partition(0, 1, 3, [], [])
    assert_kind_of KafkaSyrup::Protocol::MetadataResponse::Partition, partition
    assert_includes topic.partitions, partition
  end

  def test_that_it_encodes_correctly
    obj.add_broker(0, :hostfoo, 1300)
    obj.add_topic(0, :topicfoo).add_partition(0, 1, 3, [3, 2], [3])
    expected = [
      "\x00\x00\x00\x01",       # Number of brokers (1)
        "\x00\x00\x00\x00",     # Broker id (0)
        "\x00\x07hostfoo",      # Broker host (hostfoo)
        "\x00\x00\x05\x14",     # Broker port (1300)
      "\x00\x00\x00\x01",       # Number of topics (1)
        "\x00\x00",             # Topic error code (0)
        "\x00\x08topicfoo",     # Topic name (topicfoo)
        "\x00\x00\x00\x01",     # Number of partitions (1)
          "\x00\x00",           # Partition error code (0)
          "\x00\x00\x00\x01",   # Partition id (1)
          "\x00\x00\x00\x03",   # Partition broker (3)
          "\x00\x00\x00\x02",   # Number of replicas (2)
            "\x00\x00\x00\x03", # Replica id (3)
            "\x00\x00\x00\x02", # Replica id (2)
          "\x00\x00\x00\x01",   # Number of isr (1)
            "\x00\x00\x00\x03"  # Isr id (3)
    ].join

    assert_equal expected, obj.encode[8..-1]
  end

  def test_that_it_decodes_correctly
    obj.add_broker(0, :hostfoo, 1300)
    obj.add_topic(0, :topicfoo).add_partition(0, 1, 3, [3, 2], [3])
    assert_equal obj, KafkaSyrup::Protocol::MetadataResponse.new(StringIO.new(obj.encode))
  end

  def test_that_it_raises_response_exceptions_appropriately
    obj.add_broker(0, :hostfoo, 1300)
    obj.add_topic(3, :topicfoo).add_partition(0, 1, 3, [3, 2], [3])
    assert_raises(KafkaSyrup::KafkaResponseErrors::UnknownTopicOrPartition){ KafkaSyrup::Protocol::MetadataResponse.new(StringIO.new(obj.encode)) }

    obj.topics.clear
    obj.add_topic(0, :topicfoo).add_partition(1, 1, 3, [3, 2], [3])
    assert_raises(KafkaSyrup::KafkaResponseErrors::OffsetOutOfRange){ KafkaSyrup::Protocol::MetadataResponse.new(StringIO.new(obj.encode)) }
  end
end
