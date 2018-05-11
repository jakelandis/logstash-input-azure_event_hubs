# encoding: utf-8
require "logstash/util/loggable"
module LogStash
  module Inputs
    module Azure
      class Processor
        include LogStash::Util::Loggable
        include com.microsoft.azure.eventprocessorhost.IEventProcessor

        def initialize(queue, codec, auto_commit_interval_ms, decorator, meta_data)
          @queue = queue
          @codec = codec
          @auto_commit_interval_ms = auto_commit_interval_ms
          @last = Time.now.to_f * 1000
          @decorator = decorator
          @meta_data = meta_data
          @logger = self.logger

        end

        def onOpen(context)
          @logger.info("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} is opening.")
        end

        def onClose(context, reason)
          @logger.info("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} is closing. (reason=#{reason.to_s})")
        end

        def onEvents(context, batch)
          @logger.debug("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} is processing a batch.") if @logger.debug?
          last_payload = nil
          batch_size = 0
          batch.each do |payload|

            bytes = payload.getBytes
            batch_size += bytes.size
            @logger.trace("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s}, Offset: #{payload.getSystemProperties.getOffset.to_s},"+
                              " Sequence: #{payload.getSystemProperties.getSequenceNumber.to_s}, Size: #{bytes.size}") if @logger.trace?

            @codec.decode(bytes.to_a.pack('C*')) do |event|

              @decorator.call(event)
              if @meta_data
                event.set("[@metadata][azure_event_hub][name]", context.getEventHubPath)
                event.set("[@metadata][azure_event_hub][consumer_group]", context.getConsumerGroupName)
                event.set("[@metadata][azure_event_hub][partition]", context.getPartitionId)
                event.set("[@metadata][azure_event_hub][offset]", payload.getSystemProperties.getOffset)
                event.set("[@metadata][azure_event_hub][sequence]", payload.getSystemProperties.getSequenceNumber)
                event.set("[@metadata][azure_event_hub][timestamp]",payload.getSystemProperties.getEnqueuedTime.getEpochSecond)
                event.set("[@metadata][azure_event_hub][event_size]", bytes.size)
              end
              @queue << event
              if @auto_commit_interval_ms >= 0
                now = Time.now.to_f * 1000
                since_last_check_point = now - @last
                if since_last_check_point >= @auto_commit_interval_ms
                  context.checkpoint(payload).get
                  @last = now
                end
              end
            end
            last_payload = payload
          end

          @codec.flush
          #always create checkpoint at end of onEvents in case of sparse events
          context.checkpoint(last_payload).get if last_payload
          @logger.debug("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} finished processing a batch of #{batch_size} bytes.") if @logger.debug?
        end

        def onError(context, error)
          @logger.error("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} experienced an error #{error.to_s})")
        end
      end
    end
  end
end





