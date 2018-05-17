# encoding: utf-8
require "logstash/util/loggable"
java_import java.util.function.Consumer

module LogStash
  module Inputs
    module Azure
      class ErrorNotificationHandler
        include Consumer
        include LogStash::Util::Loggable

        def initialize
          @logger = self.logger
        end

        # not sure exactly when this will be called (instead of the other exception handlers), but this is how the exception handling is modeling in the example code.
        def accept(exception_received_event_args)
          @logger.error("Error with Event Processor Host #{exception_received_event_args.to_s}")
        end
      end
    end
  end
end
