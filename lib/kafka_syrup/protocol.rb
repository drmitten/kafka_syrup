%w[
  errors
  base
  request
  response
  message
  message_set
  metadata_request
  metadata_response
  produce_request
  produce_response
  fetch_request
  fetch_response
  offset_request
  offset_response
].each{ |file| require "kafka_syrup/protocol/#{file}" }

module KafkaSyrup
  module Protocol
    REPLICA_ID = -1
  end
end
