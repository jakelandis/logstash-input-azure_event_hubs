# encoding: utf-8
require "logstash-input-azure_event_hubs"
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "logstash/inputs/processor_factory"
require "logstash/inputs/error_notification_handler"
require "logstash/inputs/named_thread_factory"


java_import com.microsoft.azure.eventhubs.ConnectionStringBuilder
java_import com.microsoft.azure.eventprocessorhost.EventProcessorHost
java_import com.microsoft.azure.eventprocessorhost.EventProcessorOptions
java_import com.microsoft.azure.eventprocessorhost.InMemoryCheckpointManager
java_import com.microsoft.azure.eventprocessorhost.InMemoryLeaseManager
java_import com.microsoft.azure.eventprocessorhost.HostContext
java_import java.util.concurrent.Executors
java_import java.util.concurrent.TimeUnit

class LogStash::Inputs::AzureEventHub < LogStash::Inputs::Base
  config_name "azure_event_hubs"
  config :event_hubs, :validate => :array, :required => true
  config :auto_commit_interval_ms, :validate => :number, :default => 5000

  def register
    # TODO: check the event hubs content and echo non-sensitive config
    # at least 2 threads
    # TODO: protect the inner connection strings by removing the sensitive data, likely use a custom validator to ensure early lifecyle
    #TODO: expose these options: EventProcessorOptions
    # TODO: put single_node behind a flag and only allow in memory offsefts for this mode.

  end

  def run(queue)
    event_hub_threads = []
    @event_hubs.each do |event_hub|
      event_hub_threads << Thread.new do
        thread = Thread.current
        connection_string = event_hub['connection_string']
        connection_string_oracle = ConnectionStringBuilder.new(connection_string)
        event_hub_name = connection_string_oracle.get_event_hub_name
        java.lang.Thread.currentThread.setName(event_hub_name + "-main")
        consumer_group = event_hub['consumer_group'] || '$Default'
        threads = event_hub['threads'] || 2
        storage_connection_string = event_hub['storage_connection_string']
        named_thread_factory = LogStash::Inputs::Azure::NamedThreadFactory.new(event_hub_name + "-worker")
        scheduled_executor_service = Executors.newScheduledThreadPool([threads, 2].max, named_thread_factory)
        thread[:executor_service] = scheduled_executor_service
        if storage_connection_string
          event_processor_host = EventProcessorHost.new(
              EventProcessorHost.createHostName('logstash'),
              event_hub_name,
              consumer_group,
              connection_string,
              storage_connection_string,
              event_hub_name,
              scheduled_executor_service)

        else
          @logger.warn("Event Hub: #{event_hub_name} WARNING! - You have NOT specified a `storage_connection_string`. " +
                           "This configuration is only supported for reading the entire contents of an Event Hub with a single Logstash instance.")

          checkpoint_manager = InMemoryCheckpointManager.new
          lease_manager = InMemoryLeaseManager.new
          event_processor_host = EventProcessorHost.new(
              EventProcessorHost.createHostName('logstash'),
              event_hub_name,
              consumer_group,
              connection_string,
              checkpoint_manager,
              lease_manager,
              scheduled_executor_service,
              nil)
          lease_manager.java_send :initialize, [HostContext], event_processor_host.getHostContext
          checkpoint_manager.java_send :initialize, [HostContext], event_processor_host.getHostContext
        end

        options = EventProcessorOptions.new
        options.setExceptionNotification(LogStash::Inputs::Azure::ErrorNotificationHandler.new)
        event_processor_host.registerEventProcessorFactory(LogStash::Inputs::Azure::ProcessorFactory.new(queue, @codec.clone, @auto_commit_interval_ms, self.method(:decorate)), options)
            .whenComplete {|x, e|
              @logger.info("Registration complete")
              @logger.error("Failure while registering.", :exception => e, :backtrace => e.backtrace) if e
            }
            .then_accept {|x|
              @logger.info("Event Hub: #{event_processor_host.getHostContext.getEventHubPath.to_s} processing events... ")
              # this blocks the completable future running on the event_hub_name-main thread
              while !stop?
                Stud.stoppable_sleep(1) {stop?}
              end
            }
            .thenCompose {|x|
              @logger.info("Unregistering.... ")
              event_processor_host.unregisterEventProcessor
            }
            .exceptionally {|e|
              @logger.error("Event Hub: #{event_processor_host.getHostContext.getEventHubPath.to_s} error.", :exception => e, :backtrace => e.backtrace) if e
              nil
            }
            .get
      end
    end

    # this blocks the input
    while !stop?
      Stud.stoppable_sleep(1) {stop?}
    end

    # ensure clean shutdown since we own the executor service
    event_hub_threads.each {|thread|
      thread[:executor_service].each do |service|
        service.shutdown
        begin
          service.awaitTermination(1, TimeUnit::MINUTES);
        rescue => e
          @logger.debug("interrupted while waiting to close executor service, this can generally be ignored", :exception => e, :backtrace => e.backtrace) if e
        end
      end
      thread.join
    }
  end

end
