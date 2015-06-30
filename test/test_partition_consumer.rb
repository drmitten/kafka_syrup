require 'minitest_helper'
require 'responses'

class TestPartitionConsumer < Minitest::Test
  include Responses

  def setup
    Socket.any_instance.stubs(connect: true)
    KafkaSyrup.stubs(get_metadata: metadata_response)
    @obj = KafkaSyrup::PartitionConsumer.new(topic: :foo, partition: 2, max_bytes: 1024)
    @obj.broker.stubs(:send_request).with{ |r| r.is_a? KafkaSyrup::Protocol::OffsetRequest }.returns(offset_response)
    @obj.broker.stubs(:send_request).with{ |r| r.is_a? KafkaSyrup::Protocol::FetchRequest }.returns(fetch_response)
  end

  def stub_broker_socket
      @obj.broker.unstub(:send_request)
      @obj.broker.stubs(socket: StringIO.new(fetch_response.encode))
      @obj.broker.socket.stubs(connect: true, write: true)
      @obj.offset = 1300
  end

  def test_refresh_metadata_retrieves_metadata_from_server
    KafkaSyrup.expects(:get_metadata).with(:foo).returns(metadata_response)
    @obj.refresh_metadata
  end

  def test_refresh_metadata_sets_broker
    @obj.broker = nil
    @obj.refresh_metadata
    refute_nil @obj.broker
  end

  def test_broker_has_communication_methods
    @obj.broker = nil
    @obj.refresh_metadata
    assert @obj.broker.respond_to?(:send_request)
  end

  def test_refresh_metadata_raises_exception_when_topic_not_found
    metadata_response.topics.stubs(detect: nil)
    assert_raises(KafkaSyrup::TopicNotFound){ @obj.refresh_metadata }
  end

  def test_refresh_metadata_raises_exception_when_partition_not_found
    metadata_response.topics.first.partitions.stubs(detect: nil)
    assert_raises(KafkaSyrup::PartitionNotFound){ @obj.refresh_metadata }
  end

  def test_refresh_metadata_raises_exception_when_broker_not_found
    metadata_response.brokers.stubs(detect: nil)
    assert_raises(KafkaSyrup::BrokerNotFound){ @obj.refresh_metadata }
  end

  def test_refresh_metadata_closes_existing_broker
    @obj.broker = stub(socket: mock(:close))
    @obj.refresh_metadata
  end

  def test_get_available_offset_gets_offset_from_server
    @obj.broker.expects(:send_request).with(offset_request).returns(offset_response)
    assert_equal 1300, @obj.get_available_offset
  end

  def test_get_available_offset_honors_earliest
    offset_request.topics.first.partitions.first.time = -2
    @obj.broker.expects(:send_request).with(offset_request).returns(offset_response)
    @obj.get_available_offset(:earliest)
  end

  def test_fetch_from_broker_sends_appropriate_request_to_server_and_returns_result
    @obj.offset = 1100
    @obj.broker.expects(:send_request).with(fetch_request).returns(fetch_response)
    @obj.fetch_from_broker
  end

  def test_fetch_from_broker_assigns_offset_if_offset_not_a_number
    @obj.expects(:get_available_offset).returns(13)
    @obj.fetch_from_broker
  end

  def test_fetch_from_broker_assigns_offset_after_fetching
    stub_broker_socket
    @obj.fetch_from_broker
    assert_equal 1303, @obj.offset
  end

  def test_fetch_from_broker_resets_offset_if_out_of_range
    @obj.offset = 3
    @obj.broker.stubs(:send_request).with{ |r| r.is_a? KafkaSyrup::Protocol::FetchRequest }.raises(KafkaSyrup::KafkaResponseErrors::OffsetOutOfRange).then.returns(fetch_response)
    @obj.fetch_from_broker
    assert_equal 1300, @obj.offset
  end

    def test_fetch_from_broker_processes_messages_as_they_come_in
      stub_broker_socket
      tmp = []
      @obj.fetch_from_broker do |msg|
        tmp << msg
      end
      refute_empty tmp
    end

    def test_fetch_starts_fetcher_thread_if_necessary
      stub_broker_socket
      assert_nil @obj.thread
      @obj.fetch
      assert_kind_of Thread, @obj.thread
    end

    def test_fetch_retrieves_messages_from_queue_if_available
      @obj.expects(:fetch_from_broker).never
      @obj.messages.push partition: 1, offset: 1302, message: 'baz'
      assert_equal 1, @obj.fetch.count
    end

    def test_fetch_fills_messages_queue_when_queue_empty
      stub_broker_socket
      assert_includes @obj.fetch, { partition: 2, offset: 1301, message: 'bar' }
    end

    def test_fetch_honors_limit
      stub_broker_socket
      assert_equal 1, @obj.fetch(1).count
    end

    def test_fetch_stores_unretrieved_messages_in_queue
      stub_broker_socket
      assert_empty @obj.messages
      @obj.fetch(1)
      sleep 0.1
      refute_empty @obj.messages
    end

    def test_fetch_sets_offset_to_last_message_retrieved
      stub_broker_socket
      assert_equal 1300, @obj.offset
      @obj.fetch(1)
      assert_equal 1302, @obj.offset
    end
end
