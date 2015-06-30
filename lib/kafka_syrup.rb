%w[
  errors
  utils
  encoding
  protocol
  version
  broker
  topic_producer
  partition_consumer
  topic_consumer
].each{ |file| require "kafka_syrup/#{file}" }

module KafkaSyrup
  E = Encoding # Just to abbreviate the typing

  Configuration = Struct.new :produce_required_acks, :produce_timeout, :consume_max_wait_time, :consume_min_bytes, :consume_max_bytes, :so_sndbuf,
   :brokers, :zookeeper_hosts, :zookeeper_path, :retry_backoff, :logger do

    def with_defaults
      self.produce_required_acks = -1
      self.produce_timeout = 1500
      self.consume_max_wait_time = 100
      self.consume_min_bytes = 1
      self.consume_max_bytes = 1024 * 1024
      self.so_sndbuf = 100 * 1024
      self.brokers = ''
      self.zookeeper_path = '/'
      self.retry_backoff = 10 * 1000
      self
    end

    def logger
      return @logger if @logger
      @logger = Logger.new(STDOUT)
      @logger.formatter = ->(severity, datetime, progname, msg){ "[#{datetime}] #{severity} : #{msg}\n" }
      @logger.level = Logger::WARN
      @logger
    end
  end

  class << self
    def configure
      yield config
    end

    def config
      @config ||= Configuration.new.with_defaults
    end

    def brokers
      @brokers ||= config.brokers.split(',').map(&:strip).map{ |info| Broker.new(*info.split(':')) }
    end

    def get_metadata(*topics)
      request = KafkaSyrup::Protocol::MetadataRequest.new(*topics)

      brokers.each do |broker|
        begin
          response = broker.send_request(request, close: true)
          return response
        rescue StandardError
        end
      end

      raise NoBrokers
    end
  end
end
