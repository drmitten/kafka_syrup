module Responses
  def metadata_request
    @metadata_request ||= KafkaSyrup::Protocol::MetadataRequest.new(:foo)
  end

  def metadata_response
    return @metadata_response if @metadata_response
    @metadata_response = KafkaSyrup::Protocol::MetadataResponse.new
    @metadata_response.add_broker(1, 'localhost', 13)
    @metadata_response.add_broker(2, 'localhost', 14)
    @metadata_response.add_topic(0, 'foo').tap do |t|
      t.add_partition(0, 0, 1, [], [])
      t.add_partition(0, 1, 2, [], [])
      t.add_partition(0, 2, 1, [], [])
      t.add_partition(0, 3, 2, [], [])
      t.add_partition(0, 4, 1, [], [])
    end
    @metadata_response
  end

  def offset_request
    return @offset_request if @offset_request
    @offset_request = KafkaSyrup::Protocol::OffsetRequest.new
    @offset_request.add_topic(:foo).add_partition(2)
    @offset_request
  end

  def offset_response
    return @offset_response if @offset_response
    @offset_response = KafkaSyrup::Protocol::OffsetResponse.new
    @offset_response.add_topic('foo').tap do |topic|
      topic.add_partition(0, 0, [1000])
      topic.add_partition(1, 0, [1100])
      topic.add_partition(2, 0, [1300])
      topic.add_partition(3, 0, [1400])
      topic.add_partition(4, 0, [1500])
    end
    @offset_response
  end

  def fetch_request
    return @fetch_request if @fetch_request
    @fetch_request = KafkaSyrup::Protocol::FetchRequest.new(max_bytes: 1024)
    @fetch_request.add_topic(:foo).add_partition(2, 1100)
    @fetch_request
  end

  def fetch_response
    return @fetch_response if @fetch_response
    @fetch_response = KafkaSyrup::Protocol::FetchResponse.new
    @fetch_response.add_topic('foo').tap do |topic|
      topic.add_partition(2, 0, 1302).tap do |partition|
        partition.add_message('bar', offset: 1301)
        partition.add_message('baz', offset: 1302)
      end
      topic.add_partition(3, 0, 1402).tap do |partition|
        partition.add_message('howdy', offset: 1401)
        partition.add_message('partner', offset: 1402)
      end
    end
    @fetch_response
  end
end
