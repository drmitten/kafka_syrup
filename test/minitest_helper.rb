$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'kafka_syrup'
require 'minitest/autorun'
require 'minitest/reporters'
Minitest::Reporters.use! Minitest::Reporters::DefaultReporter.new

require 'mocha/mini_test'
require 'pry'
require 'logger'
