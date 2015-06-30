require 'minitest_helper'
require 'responses'

class TestTopicProducer < Minitest::Test
  include Responses

  def setup
    KafkaSyrup.stubs(get_metadata: metadata_response)
    Socket.any_instance.stubs(connect: true)

    @obj = KafkaSyrup::TopicProducer.new(:foo, timeout: 13)

    @obj.partitioner = ->(msg) { 1 }
  end

  def setup_refresh_metadata
    @obj.brokers.clear
    @obj.partitions.clear
  end

  def setup_send_message
    @obj.brokers.values.each do |b|
      b.stubs send_request: :baz
    end

    @obj.stubs refresh_metadata: true
  end

  def test_that_initialize_populates_brokers
    assert_equal metadata_response.brokers, @obj.brokers.values
  end

  def test_that_brokers_have_communication_methods
    assert @obj.brokers.values.all?{ |b| b.respond_to?(:send_request) }
  end

  def test_that_initialize_populates_partitions
    assert_equal metadata_response.topics.first.partitions, @obj.partitions.values
  end

  def test_that_initialize_sets_a_partitioner
    assert @obj.partitioner.is_a?(Proc)
  end

  def test_that_initialize_stores_options_for_sending_later
    assert_equal({ timeout: 13 }, @obj.opts)
  end

  def test_that_refresh_metadata_response_closes_existing_broker_sockets
    setup_refresh_metadata
    @obj.brokers.store 1, KafkaSyrup::Broker.new('localhost', 15)
    @obj.brokers.values.first.socket.expects(:close)
    @obj.refresh_metadata
  end

  def test_that_refresh_metadata_populates_brokers
    setup_refresh_metadata
    assert_empty @obj.brokers
    @obj.refresh_metadata
    assert_equal metadata_response.brokers, @obj.brokers.values
  end

  def test_that_refresh_metadata_populates_partitions
    setup_refresh_metadata
    assert_empty @obj.partitions
    @obj.refresh_metadata
    assert_equal metadata_response.topics.first.partitions, @obj.partitions.values
  end

  def test_that_send_message_uses_the_partitioner_to_choose_an_id
    setup_send_message
    @obj.partitioner.expects(:call).with('foo').returns(1)
    @obj.send_message('foo')
  end

  def test_that_send_message_sends_the_correct_message_to_the_correct_broker
    setup_send_message
    req = KafkaSyrup::Protocol::ProduceRequest.new
    req.add_topic(:foo).add_partition(1).add_message(:bar)
    @obj.brokers[2].expects(:send_request).with{ |var| var == req }.returns(3)
    @obj.send_message(:bar)
  end

  def test_that_send_message_handles_NotLeaderForPartition
    setup_send_message
    @obj.brokers[2].stubs(:send_request).raises(KafkaSyrup::KafkaResponseErrors::NotLeaderForPartition).returns(:baz)
    @obj.expects(:refresh_metadata)
    assert_equal :baz, @obj.send_message(:foo)
  end

  def test_that_send_message_handles_SocketReadError
    setup_send_message
    @obj.brokers[2].stubs(:send_request).raises(KafkaSyrup::SocketReadError).returns(:baz)
    @obj.expects(:refresh_metadata)
    assert_equal :baz, @obj.send_message(:foo)
  end
end
