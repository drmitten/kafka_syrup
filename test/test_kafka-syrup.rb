require 'minitest_helper'
require 'responses'

class TestKafkaSyrup < Minitest::Test
  include Responses

  def test_that_it_has_a_version_number
    refute_nil KafkaSyrup::VERSION
  end

  def test_it_has_config
    refute_nil KafkaSyrup.config
  end

  def test_it_has_default_config_values
    assert_equal(-1, KafkaSyrup.config.produce_required_acks)
  end

  def test_brokers
    # Clear out the cached variable since other tests may have set it
    KafkaSyrup.instance_variable_set('@brokers', nil)
    KafkaSyrup.config.brokers = 'localhost:12, localhost:14, localhost:15'
    assert_equal 3, KafkaSyrup.brokers.count
    assert KafkaSyrup.brokers.all?{ |b| b.is_a?(KafkaSyrup::Broker) }
  end

  class TestGetMetadataMethod < Minitest::Test
    include Responses

    def setup
      KafkaSyrup.brokers.clear
      KafkaSyrup.brokers << stub(socket: true, send_request: metadata_response)
      KafkaSyrup.brokers << stub(socket: true, send_request: metadata_response)
    end

    def test_it_sends_a_metadata_request
      KafkaSyrup.brokers.first.expects(:send_request).with(metadata_request, close: true)
      KafkaSyrup.get_metadata(:foo)
    end

    def test_it_returns_a_metadata_response
      assert KafkaSyrup.get_metadata.is_a?(KafkaSyrup::Protocol::MetadataResponse)
    end

    def test_it_raises_exception_if_unable_to_retrieve_metadata_response
      KafkaSyrup.brokers.each{ |b| b.stubs(:send_request).raises(StandardError) }
      assert_raises(KafkaSyrup::NoBrokers){ KafkaSyrup.get_metadata }
    end
  end
end
