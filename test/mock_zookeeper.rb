require 'multi_json'

module ZK
  class << self
    def new(*args)
      Client::Threaded.new
    end
  end

  module Client
    class Threaded
      attr_accessor :connected, :watcher

      alias :connected? :connected

      def initialize
        self.connected = true
      end

      def get(path)
        case path
        when '/consumers/groupfoo/ids/consumerx'
          [MultiJson.dump({ pattern: :static,
            version: 1,
            subscription: { bar: 1 }
          }), :metadata]
        when /^\/consumers\/groupfoo\/ids\/.*/
          [MultiJson.dump({ pattern: :static,
            version: 1,
            subscription: { foo: 1 }
          }), :metadata]
        end
      end

      def children(*args)
        path, *_ = args
        case path
        when '/consumers/groupfoo/ids'
          ['consumera', 'consumerfoo', 'consumerx', 'consumerz']
        end
      end

      def mkdir_p(*args)
        true
      end

      def rm_rf(*args)
        true
      end

      def register(*args, &block)
        self.watcher = block
        block
      end

      def create(*args)
        true
      end

      def set(*args)
        true
      end
    end
  end
end
