# encoding: utf-8
require "logstash-input-azure_event_hubs"
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "logstash/inputs/processor_factory"
require "logstash/inputs/error_notification_handler"
require "logstash/inputs/named_thread_factory"
require "logstash/inputs/look_back_position_provider"


java_import com.microsoft.azure.eventprocessorhost.EventProcessorHost
java_import com.microsoft.azure.eventprocessorhost.EventProcessorOptions
java_import com.microsoft.azure.eventprocessorhost.InMemoryCheckpointManager
java_import com.microsoft.azure.eventprocessorhost.InMemoryLeaseManager
java_import com.microsoft.azure.eventprocessorhost.HostContext
java_import java.util.concurrent.Executors
java_import java.util.concurrent.TimeUnit


class LogStash::Inputs::AzureEventHub < LogStash::Inputs::Base
  config_name "azure_event_hubs"

  # The event hubs to connect read from. This configuration supports two formats. The simple array form that assumes all of the other configurations options apply uniformly across all event hubs.
  # [
  #   "event_hub_name1" , "event_hub_name2"
  #  ]
  # Alternatively, the supported configuration options may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "event_hub_connection" => "Endpoint=sb://example1..."
  #  },
  #  "event_hub_name2" => {
  #      "event_hub_connection" => "Endpoint=sb://example2..."
  #  },
  # ]
  config :event_hubs, :validate => :array, :required => true

  # The connection used for all of the event hubs.
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "event_hub_connection" => "Endpoint=sb://example..."
  #  }
  # ]
  config :event_hub_connection, :validate => :password, :required => true

  # Used to persists the offsets between restarts and ensure that multiple instances of Logstash process different partitions
  # This is *stongly* encouraged to be set for production environments.
  # When this value is set, restarts will pick up from where it left off. Without this value set the initial_position is *always* used.
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "storage_connection" => "DefaultEndpointsProtocol=https;AccountName=example...."
  #  }
  # ]
  config :storage_connection, :validate => :password, :required => false

  # The storage container to persist the offsets.
  # Defaults to the event hub name. Note - if multiple event hubs share a name, storage connection, and consumer group this MUST be set per event hub else the offsets will persisted incorrectly.
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "storage_container" => "event_hub_name1"
  #  }
  # ]
  config :storage_container, :validate => :string, :required => false

  # Total threads used process events. Requires at minimum 2 threads.
  config :threads, :validate => :number, :default => 4

  # Consumer group used by all configured event hubs. All Logstash instances should use the same consumer group. It is recommended to change from the $Defualt to a consumer group specifically for Logstash.
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "consumer_group" => "logstash"
  #  }
  # ]
  config :consumer_group, :validate => :string, :default => '$Default'

  # The max size of events are processed together. A checkpoint is created after each batch. Increasing this value may help with performance, but requires more memory.
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "max_batch_size" => 50
  #  }
  # ]
  config :max_batch_size, :validate => :number, :default => 50

  # The max size of events that are retrieved prior to processing. Increasing this value may help with performance, but requires more memory.
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "prefetch_count" => 300
  #  }
  # ]
  config :prefetch_count, :validate => :number, :default => 300

  # The max time allowed receive events without a timeout.
  # Value is expressed in seconds
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "receive_timeout" => 60
  #  }
  # ]
  config :receive_timeout, :validate => :number, :default => 60

  # When first reading from an event hub, start from this position.
  # HEAD - reads ALL pre-existing events in the event hub
  # TAIL - reads NO pre-existing events in the event hub
  # LOOK_BACK - reads TAIL minus N seconds worth of pre-existing events
  # Note - If the storage_connection is set, this configuration is only applicable for the very first time Logstash reads from the event hub.
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "initial_position" => "HEAD"
  #  }
  # ]
  config :initial_position, :validate => ['HEAD', 'TAIL', 'LOOK_BACK', 'head', 'tail', 'look_back'], :default => 'HEAD'

  # The number of seconds to look back for pre-existing events to determine the initial position.
  # Note - If the storage_connection is set, this configuration is only applicable for the very first time Logstash reads from the event hub.
  # Note - this options is only used when initial_position => "LOOK_BACK"
  # Value is expressed in seconds
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "initial_position" => "LOOK_BACK"
  #      "initial_position_look_back" => 86400
  #  }
  # ]
  config :initial_position_look_back, :validate => :number, :required => false

  # The interval in seconds between writing checkpoint while processing a batch. Default 5 seconds. Checkpoints can slow down processing, but are needed to know where to start after a restart.
  # Note - checkpoints happen after every batch, so this configuration is only applicable while processing a single batch.
  # Value is expressed in seconds, set to zero to disable
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "checkpoint_interval" => 5
  #  }
  # ]
  config :checkpoint_interval, :validate => :number, :default => 5

  # Adds meta data to the event.
  # [@metadata][azure_event_hubs][name] - the name of hte event host
  # [@metadata][azure_event_hubs][consumer_group] - the consumer group that consumed this event
  # [@metadata][azure_event_hubs][processor_host] - the unique identifier that identifies which host processed this event. Note - there can be multiple processor hosts on a single instance of Logstash.
  # [@metadata][azure_event_hubs][partition] - the partition from which event came from
  # [@metadata][azure_event_hubs][offset] - the event hub offset for this event
  # [@metadata][azure_event_hubs][sequence] - the event hub sequence for this event
  # [@metadata][azure_event_hubs][timestamp] - the enqueued time of the event
  # [@metadata][azure_event_hubs][event_size] - the size of the event
  # Alternatively, this configuration may be also be expressed per event hub
  # Alternatively, this configuration may be also be expressed per event hub
  # [
  #   "event_hub_name1" => {
  #      "decorate_events" => true
  #  }
  # ]
  config :decorate_events, :validate => :boolean, :default => false


  def initialize(params)

    @event_hubs_exploded = []
    # explode the parameters to be scoped per event_hub, prefer any configuration already scoped over the globally scoped config
    global_config = {}
    params.each do |k, v|
      if !k.eql?('id') && !k.eql?('event_hubs')&& !k.eql?('threads') # don't copy these to the per-event-hub configs
        global_config[k] = v
      end
    end

    params['event_hubs'].each do |event_hub|
      if event_hub.is_a?(String)
        @event_hubs_exploded << {'event_hubs' => [event_hub]}.merge(global_config)
      elsif event_hub.is_a?(Hash)
        event_hub.each do |event_hub_name, config|
          config.each do |k, v|
            if 'event_hub_connection'.eql?(k) || 'storage_connection'.eql?(k)
              config[k] = ::LogStash::Util::Password.new(v)
            end
          end
          config.merge!({'event_hubs' => [event_hub_name]})
          config.merge!(global_config) {|k, v1, v2| v1}
          @event_hubs_exploded << config
          # trick the global validation here, validation against the scoped config will happen later
          params['event_hub_connection'] = 'placeholder' unless params['event_hub_connection']
        end
      else
        raise "event_hubs must be either string or hash"
      end
    end

    super(params)

    # explicitly validate all the per event hub configs, strip out the placeholder
    @event_hubs_exploded.each do |event_hub|
      event_hub['event_hub_connection'] = nil if event_hub['event_hub_connection'].eql?('placeholder')
      if !self.class.validate(event_hub)
        raise LogStash::ConfigurationError,
              I18n.t("logstash.runner.configuration.invalid_plugin_settings")
      end
    end
  end

  def register
    # augment the exploded config with the defaults
    @event_hubs_exploded.each do |event_hub|
      @config.each do |key, value|
        if !key.eql?('id') && !key.eql?('event_hubs')
          event_hub[key] = value unless event_hub[key]
        end
      end
    end
    @logger.debug("Exploded Event Hub configuration: #{@event_hubs_exploded.to_s}")
  end


  def run(queue)
    event_hub_threads = []
    named_thread_factory = LogStash::Inputs::Azure::NamedThreadFactory.new("azure_event_hubs-worker")
    scheduled_executor_service = Executors.newScheduledThreadPool(@threads, named_thread_factory)
    @event_hubs_exploded.each do |event_hub|
      event_hub_name = event_hub['event_hubs'].first # there will always only be 1 from @event_hubs_exploded
      @logger.info("Event Hub #{event_hub_name} is initializing... ")
      event_hub_threads << Thread.new do
        begin
          if event_hub['storage_connection']
            event_processor_host = EventProcessorHost.new(
                EventProcessorHost.createHostName('logstash'),
                event_hub_name,
                event_hub['consumer_group'],
                event_hub['event_hub_connection'].value,
                event_hub['storage_connection'].value,
                event_hub_name,
                scheduled_executor_service)
          else
            @logger.warn("You have NOT specified a `storage_connection_string` for #{event_hub_name}. This configuration is only supported for a single Logstash instance.")
            checkpoint_manager = InMemoryCheckpointManager.new
            lease_manager = InMemoryLeaseManager.new
            event_processor_host = EventProcessorHost.new(
                EventProcessorHost.createHostName('logstash'),
                event_hub_name,
                event_hub['consumer_group'],
                event_hub['event_hub_connection'].value,
                checkpoint_manager,
                lease_manager,
                scheduled_executor_service,
                nil)
            #using java_send to avoid naming conflicts with 'initialize' method
            lease_manager.java_send :initialize, [HostContext], event_processor_host.getHostContext
            checkpoint_manager.java_send :initialize, [HostContext], event_processor_host.getHostContext
          end
          options = EventProcessorOptions.new
          options.setExceptionNotification(LogStash::Inputs::Azure::ErrorNotificationHandler.new)
          case @initial_position.upcase
            when 'HEAD'
              msg = "Configuring Event Hub #{event_hub_name} to read events all events."
              @logger.debug("If this is the initial read... " + msg) if event_hub['storage_connection']
              @logger.info(msg) unless event_hub['storage_connection']
              options.setInitialPositionProvider(EventProcessorOptions::StartOfStreamInitialPositionProvider.new(options))
            when 'TAIL'
              msg = "Configuring Event Hub #{event_hub_name} to read only new events."
              @logger.debug("If this is the initial read... " + msg) if event_hub['storage_connection']
              @logger.info(msg) unless event_hub['storage_connection']
              options.setInitialPositionProvider(EventProcessorOptions::EndOfStreamInitialPositionProvider.new(options))
            when 'LOOK_BACK'
              msg = "Configuring Event Hub #{event_hub_name} to read events starting at 'now - #{@initial_position_look_back}' seconds."
              @logger.debug("If this is the initial read... " + msg) if event_hub['storage_connection']
              @logger.info(msg) unless event_hub['storage_connection']
              options.setInitialPositionProvider(LogStash::Inputs::Azure::LookBackPositionProvider.new(@initial_position_look_back))

          end

          event_processor_host.registerEventProcessorFactory(LogStash::Inputs::Azure::ProcessorFactory.new(queue, event_hub['codec'].clone, event_hub['checkpoint_interval'], self.method(:decorate), event_hub['decorate_events']), options)
              .whenComplete {|x, e|
                @logger.info("Event Hub #{event_processor_host.getHostContext.getEventHubPath.to_s} registration complete. ")
                @logger.error("Event Hub #{event_processor_host.getHostContext.getEventHubPath.to_s} failure while registering.", :exception => e, :backtrace => e.backtrace) if e
              }
              .then_accept {|x|
                @logger.info("Event Hub #{event_processor_host.getHostContext.getEventHubPath.to_s} is processing events... ")
                # this blocks the completable future chain from progressing, actual work is done via the executor service
                while !stop?
                  Stud.stoppable_sleep(1) {stop?}
                end
              }
              .thenCompose {|x|
                @logger.info("Unregistering #{event_processor_host.getHostContext.getEventHubPath.to_s}... this can take a while... ")
                event_processor_host.unregisterEventProcessor
              }
              .exceptionally {|e|
                @logger.error("Event Hub #{event_processor_host.getHostContext.getEventHubPath.to_s} encountered an error.", :exception => e, :backtrace => e.backtrace) if e
                nil
              }
              .get # this blocks till all of the futures are complete.
          @logger.info("Event Hub #{event_processor_host.getHostContext.getEventHubPath.to_s} is closed.")
        rescue => e
          @logger.error("Event Hub #{event_hub_name} failed during initialization.", :exception => e, :backtrace => e.backtrace) if e
          do_stop
        end
      end
    end

    # this blocks the input from existing. (all work is being done in threads)
    while !stop?
      Stud.stoppable_sleep(1) {stop?}
    end

    # This blocks the input till all the threads have run to completion.
    event_hub_threads.each do |thread|
      thread.join
    end

    # Ensure proper shutdown of executor service. # Note - this causes a harmless warning in the logs that scheduled tasks are being rejected.
    scheduled_executor_service.shutdown
    begin
      scheduled_executor_service.awaitTermination(10, TimeUnit::MINUTES);
    rescue => e
      @logger.debug("interrupted while waiting to close executor service, this can generally be ignored", :exception => e, :backtrace => e.backtrace) if e
    end
  end
end


