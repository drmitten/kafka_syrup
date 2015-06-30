# encoding: ASCII-8BIT

require 'minitest_helper'

class TestOffsetResponse < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Protocol::OffsetResponse.new
  end

  def test_that_add_topic_returns_the_added_topic
    assert_empty obj.topics
    t = obj.add_topic(:foo)
    assert_kind_of KafkaSyrup::Protocol::OffsetResponse::Topic, t
    assert_includes obj.topics, t
  end

  def test_that_add_partition_returns_the_added_partition
    topic = obj.add_topic(:foo)
    assert_empty topic.partitions
    p = topic.add_partition(1, 0, [49, 50])
    assert_kind_of KafkaSyrup::Protocol::OffsetResponse::Partition, p
    assert_includes topic.partitions, p
  end

  def test_that_it_encodes_correctly
    obj.add_topic(:foo).add_partition(1, 0, [49, 50])
    expected = [
      "\x00\x00\x00\x01",                       # Number of topics(1)
        "\x00\x03foo",                          # Name of topic(foo)
        "\x00\x00\x00\x01",                     # Number of partitions(1)
          "\x00\x00\x00\x01",                   # Partition ID(1)
          "\x00\x00",                           # Error code
          "\x00\x00\x00\x02",                   # Number of offsets
            "\x00\x00\x00\x00\x00\x00\x00\x31", # Offset(49)
            "\x00\x00\x00\x00\x00\x00\x00\x32"  # Offset(50)
    ].join

    assert_equal expected, obj.encode[8..-1]
  end

  def test_that_it_decodes_correctly
    obj.add_topic(:foo).add_partition(1, 0, [49, 50])
    assert_equal obj, KafkaSyrup::Protocol::OffsetResponse.new(StringIO.new(obj.encode))
  end

  def test_that_it_raises_exceptions_when_appropriate
    obj.add_topic(:foo).add_partition(1, -1, [49, 50])
    assert_raises(KafkaSyrup::KafkaResponseErrors::Unknown){ KafkaSyrup::Protocol::OffsetResponse.new(StringIO.new(obj.encode)) }
  end
end
