require 'minitest_helper'
require 'responses'
require 'mock_zookeeper'

class TestTopicConsumer < Minitest::Test
  include Responses

  def setup
    Socket.any_instance.stubs(connect: true)
    KafkaSyrup.stubs(get_metadata: metadata_response)
    @obj = KafkaSyrup::TopicConsumer.new(topic: :foo, group: :groupfoo, consumer_id: :consumerfoo)
  end

  def stub_partition_consumers
    @obj.rebalance
    broker = metadata_response.brokers[0]
    broker.extend(KafkaSyrup::Broker::Communications)
    broker.stubs(socket: StringIO.new(fetch_response.encode))
    broker.socket.stubs(write: true, connect: true)
    KafkaSyrup::PartitionConsumer.any_instance.stubs(broker: broker, get_available_offset: 13)
  end

  def stub_for_register
    @obj.stubs(current_registration_info: nil, rebalance: nil)
  end

  def stub_for_unregister
    @obj.watcher = stub(unregister: true)
    @obj.rebalance
  end

  def test_zk_returns_a_connected_zookeeper_client
    assert @obj.zk.connected?
    @obj.zk.connected = false
    assert @obj.zk.connected?
  end

  def test_group_path
    assert_equal '/consumers/groupfoo', @obj.group_path
  end

  def test_membership_path
    assert_equal '/consumers/groupfoo/ids', @obj.membership_path
  end

  def test_ownership_path
    assert_equal '/consumers/groupfoo/owners/foo', @obj.ownership_path
  end

  def test_offsets_path
    assert_equal '/consumers/groupfoo/offsets/foo', @obj.offsets_path
  end

  def test_registration_path
    assert_equal '/consumers/groupfoo/ids/consumerfoo', @obj.registration_path
  end

  def test_current_registration_info_queries_zookeeper
    assert_equal({ 'pattern' => 'static', 'version' => 1, 'subscription' => { 'foo' => 1 } }, @obj.current_registration_info)
  end

  def test_current_registration_info_returns_nil_if_node_not_found
    @obj.zk.expects(:get).raises(ZK::Exceptions::NoNode)
    assert_nil @obj.current_registration_info
  end

  def test_new_registration_info_uses_current_registration_info_if_available
    assert_equal 2, @obj.new_registration_info['subscription']['foo']
  end

  def test_new_registration_info_uses_default_registration_info_if_current_unavailable
    @obj.stubs(current_registration_info: nil)
    assert_equal({ 'pattern' => 'static', 'version' => 1, 'subscription' => { 'foo' => 1 } }, @obj.new_registration_info)
  end

  def test_registered
    assert @obj.registered?
    @obj.stubs(current_registration_info: nil)
    refute @obj.registered?
    @obj.stubs(current_registration_info: { 'subscription' => { 'foo' => 0 } })
    refute @obj.registered?
    @obj.stubs(current_registration_info: { 'subscription' => { 'foo' => 5 } })
    assert @obj.registered?
  end

  def test_register_noops_of_already_registered
    @obj.zk.expects(:mkdir_p).never
    @obj.register
  end

  def test_register_creates_zookeeper_paths_if_necessary
    stub_for_register
    @obj.zk.expects(:mkdir_p).with('/consumers/groupfoo/ids')
    @obj.zk.expects(:mkdir_p).with('/consumers/groupfoo/owners/foo')
    @obj.zk.expects(:mkdir_p).with('/consumers/groupfoo/offsets/foo')
    @obj.register
  end

  def test_register_sets_a_watch_in_zookeeper
    stub_for_register
    @obj.register
    refute_nil @obj.watcher
  end

  def test_register_creates_a_registration_node_in_zookeeper
    stub_for_register
    info = MultiJson.dump({ 'pattern' => 'static', 'version' => 1, 'subscription' => { 'foo' => 1 } })
    @obj.zk.expects(:create).with('/consumers/groupfoo/ids/consumerfoo', info, ephemeral: true)
    @obj.register
  end

  def test_register_handles_node_exists
    stub_for_register
    @obj.zk.expects(:create).raises(ZK::Exceptions::NodeExists)
    @obj.zk.expects(:set)
    @obj.register
  end

  def test_register_handles_no_node
    stub_for_register
    @obj.zk.expects(:create).times(2).raises(ZK::Exceptions::NodeExists).then.returns(true)
    @obj.zk.expects(:set).raises(ZK::Exceptions::NoNode)
    @obj.register
  end

  def test_unregister_removes_the_watch_in_zookeeper
    stub_for_unregister
    @obj.watcher.expects(:unregister)
    @obj.unregister
  end

  def test_unregister_kills_fetcher_threads
    stub_for_unregister
    @obj.threads.each{ |t| t.expects(:kill) }
    @obj.unregister
  end

  def test_unregister_clears_threads
    stub_for_unregister
    refute_empty @obj.threads
    @obj.unregister
    assert_empty @obj.threads
  end

  def test_unregister_releases_partitions
    stub_for_unregister
    @obj.expects(:release_partitions)
    @obj.unregister
  end

  def test_unregister_removes_node_in_zookeeper
    stub_for_unregister
    @obj.stubs(:release_partitions)
    @obj.zk.expects(:rm_rf).with('/consumers/groupfoo/ids/consumerfoo')
    @obj.unregister
  end

  def test_subscribers
    assert_equal ['consumera', 'consumerfoo', 'consumerz'], @obj.subscribers
  end

  def test_release_partitions_removes_ownership_from_zookeeper
    @obj.partitions << 13
    @obj.zk.expects(:rm_rf).with('/consumers/groupfoo/owners/foo/13')
    @obj.release_partitions
  end

  def test_release_partitions_clears_existing_partitions
    @obj.partitions << 13
    @obj.release_partitions
    assert_empty @obj.partitions
  end

  def test_release_partitions_clears_existing_offsets
    @obj.offsets = { 13 => 4 }
    @obj.release_partitions
    assert_empty @obj.offsets
  end

  def test_rebalance_raises_exception_if_not_registered
    @obj.stubs(registered?: false)
    assert_raises(KafkaSyrup::NotRegistered){ @obj.rebalance }
  end

  def test_rebalance_synchronizes_the_lock
    @obj.lock.expects(:synchronize)
    @obj.rebalance
  end

  def test_rebalance_kills_existing_threads
    @obj.threads << mock(:kill)
    @obj.rebalance
  end

  def test_rebalance_releases_partitions
    @obj.expects(:release_partitions)
    @obj.rebalance
  end

  def test_rebalance_chooses_the_correct_partitions_to_claim
    @obj.rebalance
    assert_equal [2, 3], @obj.partitions
  end

  def test_rebalance_claims_correct_partitions_in_zookeeper
    @obj.zk.expects(:create).with('/consumers/groupfoo/owners/foo/2', 'consumerfoo', ephemeral: true)
    @obj.zk.expects(:create).with('/consumers/groupfoo/owners/foo/3', 'consumerfoo', ephemeral: true)
    @obj.rebalance
  end

  def test_rebalance_handle_node_exists
    @obj.stubs(:sleep)
    @obj.zk.expects(:create).times(3).raises(ZK::Exceptions::NodeExists).then.returns(true)
    @obj.expects(:release_partitions).times(3)
    @obj.rebalance
  end

  def test_rebalance_retrieves_offsets_from_zookeeper_for_claimed_partitions
    @obj.expects(:get_offset).with(2)
    @obj.expects(:get_offset).with(3)
    @obj.rebalance
  end

  def test_rebalance_starts_fetcher_threads_for_each_partition
    assert_empty @obj.threads
    @obj.rebalance
    assert_equal 2, @obj.threads.size
  end

  def test_rebalance_populates_control_queues
    assert_empty @obj.control_queues
    @obj.rebalance
    assert_equal 2, @obj.control_queues.size
  end

  def test_get_offset_gets_offset_from_zookeeper
    @obj.zk.expects(:get).with('/consumers/groupfoo/offsets/foo/2').returns(13)
    assert_equal 13, @obj.get_offset(2)
  end

  def test_get_offset_handles_no_node
    @obj.zk.expects(:get).raises(ZK::Exceptions::NoNode)
    assert_nil @obj.get_offset(2)
  end

    def test_set_offset_sets_the_node_in_zookeeper
      @obj.zk.expects(:set).with('/consumers/groupfoo/offsets/foo/13', '1300')
      @obj.set_offset(13, 1300)
    end

    def test_set_offset_handles_no_node
      @obj.zk.expects(:set).raises(ZK::Exceptions::NoNode)
      @obj.zk.expects(:create).with('/consumers/groupfoo/offsets/foo/13', '1300')
      @obj.set_offset(13, 1300)
    end

    def test_set_offset_handles_node_exists
      @obj.zk.expects(:set).times(2).raises(ZK::Exceptions::NoNode).then.returns(true)
      @obj.zk.expects(:create).raises(ZK::Exceptions::NodeExists)
      @obj.set_offset(13, 1300)
    end

    def test_commit_sets_uncommitted_partitions
      @obj.offsets = { 2 => 10, 3 => 11, 4 => 12 }
      @obj.uncommitted_partitions = [2, 4]
      @obj.expects(:set_offset).with(2, 10)
      @obj.expects(:set_offset).with(4, 12)
      @obj.commit
    end

    def test_commit_clears_uncommitted_partitions
      @obj.offsets = { 2 => 10, 3 => 11, 4 => 12 }
      @obj.uncommitted_partitions = [2, 4]
      @obj.commit
      assert_empty @obj.uncommitted_partitions
    end

  def test_fetch_raises_exception_if_not_registered
    @obj.stubs(registered?: false)
    assert_raises(KafkaSyrup::NotRegistered){ @obj.fetch }
  end

  def test_fetch_sends_message_to_control_queues_if_necessary
    @obj.rebalance
    @obj.control_queues.each{ |q| q.expects(:push).with(:fetch) }
    # fetch in a separate thread since the fetcher threads will not actually fetch due to the expectation overriding the method
    Thread.new{ @obj.fetch }
    sleep 0.1
  end

    def test_fetch_retrieves_messages_from_queue
      @obj.messages.push partition: 2, offset: 1303, message: 'foo'
      assert_equal 'foo', @obj.fetch.first[:message]
    end

    def test_fetch_sets_offsets_for_retrieved_messages
      stub_partition_consumers
      assert @obj.offsets.values.all?{ |o| o == 0 }
      @obj.fetch
      assert @obj.offsets.values.any?{ |o| o > 0 }
      @obj.threads.each(&:kill)
    end

    def test_fetch_honors_limit_param
      stub_partition_consumers
      assert_equal 2, @obj.fetch(2).count
    end

    def test_fetch_stores_uncommitted_offsets_when_apppropriate
      stub_partition_consumers
      assert_empty @obj.uncommitted_partitions
      @obj.fetch
      refute_empty @obj.uncommitted_partitions
    end

    def test_fetch_commits_offsets_before_fetching_when_appropriate
      stub_partition_consumers
      order = sequence('fetch')
      @obj.expects(:commit).in_sequence(order).at_least_once
      @obj.lock.expects(:synchronize).in_sequence(order)
      @obj.fetch
    end

    def test_fetch_commits_offsets_immediately_when_appropriate
      stub_partition_consumers
      @obj.commit_mode = :auto
      @obj.fetch
      assert_empty @obj.uncommitted_partitions
    end
end
