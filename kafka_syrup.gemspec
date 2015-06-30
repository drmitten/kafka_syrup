# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'kafka_syrup/version'

Gem::Specification.new do |spec|
  spec.name          = "kafka_syrup"
  spec.version       = KafkaSyrup::VERSION
  spec.authors       = ['Delbert Mitten']
  spec.email         = ['drmitten@gmail.com']

  spec.summary       = %q{A high level Kafka client.}
  spec.description   = %q{A high level Kafka client that supports producer, low level consumers, and high level consumers.}
  spec.homepage      = 'https://github.com/drmitten/kafka_syrup'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency 'zk', '~> 1.9'
  spec.add_dependency 'multi_json', '~> 1.8'

  spec.add_development_dependency 'bundler', '~> 1.9'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'minitest', '~> 5.6'
  spec.add_development_dependency 'minitest-reporters', '~> 1.0'
  spec.add_development_dependency 'mocha', '~> 1.1'

  spec.add_development_dependency 'pry', '~> 0.10'
end
