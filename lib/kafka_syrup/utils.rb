module KafkaSyrup
  module Utils
    def load_args(*args)
      if args.last.is_a?(Hash)
        args.last.each do |attr, val|
          self.send("#{attr}=", val) if self.respond_to?("#{attr}=")
        end
      end
    end

    def log
      KafkaSyrup.config.logger
    end
  end
end
