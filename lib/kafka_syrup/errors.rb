module KafkaSyrup
  class Error < StandardError
    def initialize(e = nil)
      super
      set_backtrace e.backtrace if e.is_a?(StandardError)
    end
  end

  class SocketReadError < Error; end;
  class NoBrokers < Error; end;
  class TopicNotFound < Error; end;
  class PartitionNotFound < Error; end;
  class BrokerNotFound < Error; end;
  class NotRegistered < Error; end;
end
