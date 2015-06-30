# encoding: ASCII-8BIT

require 'minitest_helper'

class TestProduceResponse < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::ProduceResponse.new
  end

  def topic
    @topic ||= KafkaSyrup::Protocol::ProduceResponse::Topic.new(:foo, [])
  end

  def test_that_add_topic_returns_the_added_topic
    assert_empty obj.topics
    t = obj.add_topic(:foo)
    assert_kind_of KafkaSyrup::Protocol::ProduceResponse::Topic, t
    assert_includes obj.topics, t
  end

  def test_that_add_partition_returns_the_added_partition
    assert_empty topic.partitions
    p = topic.add_partition(13, 0, 567)
    assert_kind_of KafkaSyrup::Protocol::ProduceResponse::Partition, p
    assert_includes topic.partitions, p

  end

  def test_that_it_encodes_correctly
    obj.add_topic(:foo).add_partition(1, 0, 13)
    expected = [
      "\x00\x00\x00\x01",                     # Number of topics(1)
        "\x00\x03foo",                        # Topic name(foo)
        "\x00\x00\x00\x01",                   # Number of partitions(1)
          "\x00\x00\x00\x01",                 # Partition ID (1)
          "\x00\x00",                         # Partition error code(0)
          "\x00\x00\x00\x00\x00\x00\x00\x0D"  # Partition offset(13)
    ].join

    assert_equal expected, obj.encode[8..-1]
  end

  def test_that_it_decodes_correctly
    obj.add_topic(:foo).add_partition(3, 0, 78)
    assert_equal obj, KafkaSyrup::Protocol::ProduceResponse.new(StringIO::new(obj.encode))
  end

  def test_that_it_raises_exceptions_appropriately
    obj.add_topic(:foo).add_partition(3, 2, 78)
    assert_raises(KafkaSyrup::KafkaResponseErrors::InvalidMessage) do
      KafkaSyrup::Protocol::ProduceResponse.new(StringIO.new(obj.encode))
    end
  end
end
