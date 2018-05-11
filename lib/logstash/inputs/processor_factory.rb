# encoding: utf-8
require "logstash/inputs/processor"
module LogStash
  module Inputs
    module Azure
      class ProcessorFactory
        include com.microsoft.azure.eventprocessorhost.IEventProcessorFactory

        def initialize(queue, codec, auto_commit_interval_ms, decorator, meta_data)
          @queue = queue
          @codec = codec
          @auto_commit_interval_ms = auto_commit_interval_ms
          @decorator = decorator
          @meta_data = meta_data
        end

        def createEventProcessor(context)
          Processor.new(@queue, @codec, @auto_commit_interval_ms, @decorator, @meta_data)
        end

      end
    end
  end
end



