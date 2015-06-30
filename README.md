# KafkaSyrup

KafkaSyrup is a Kafka client compatible with the Kafka 0.8 API and
above.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'kafka_syrup'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install kafka-syrup

## Usage

### Configuration

```ruby
require 'kafka_syrup'

KafkaSyrup.configure do |config|
  config.brokers = 'localhost:9092,localhost:9093,localhost:9094'

  # For consuming in a group
  config.zookeeper_hosts = 'localhost:2181'
end
```

*Additional configuration options can be found in the main
kafka_syrup.rb file*

### Sending messages to Kafka

```ruby
producer = KafkaSyrup::Producer.new(topic: :foo)

producer.send_message('hello world')
```

### Consuming messages from a single Kafka partition

```ruby
consumer = KafkaSyrup::PartitionConsumer.new(topic: :foo, partition: 1)

consumer.fetch

# It is possible to limit the number of messages returned:
consumer.fetch(50)
```

*Note that regardless of the limit, fetch() will block until it has received at least one
message.*

### Consuming messages in a group

```ruby
consumer = KafkaSyrup::TopicConsumer.new(topic: :foo, group: :bar)

consumer.fetch

# It is possible to limit the number of messages returned:
consumer.fetch(50)
```

*Note that regardless of the limit, fetch() will block until it has received at least one
message.*

The topic consumer utilizes zookeeper for coordination with other
members of the group and is fully compatible with the normal Kafka high
level consumer. (ie - kafk_syrup clients and java kafka clients can
coexist in the same group with no problem.)
