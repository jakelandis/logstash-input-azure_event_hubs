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
java_import com.microsoft.azure.eventhubs.ConnectionStringBuilder
java_import java.util.concurrent.Executors
java_import java.util.concurrent.TimeUnit


class LogStash::Inputs::AzureEventHubs < LogStash::Inputs::Base
  config_name "azure_event_hubs"

  # This plugin supports two styles of configuration
  # BASIC - You supply a list of Event Hub connection strings complete with the 'EntityPath' that defines the Event Hub name. All other configuration is shared.
  # ADVANCED - You supply a list of Event Hub names, and under each name provide that Event Hub's configuration. Most all of the configuration options are identical as the BASIC model, except they are configured per Event Hub.
  # Defaults to BASIC
  # Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"  , "Endpoint=sb://example2...;EntityPath=event_hub_name2"  ]
  # }
  config :config_mode, :validate => ['BASIC', 'ADVANCED', 'basic', 'advanced'], :default => 'BASIC'

  # ADVANCED MODE ONLY - The event hubs to read from. This is a array of hashes, where the each entry of the array is a hash of the event_hub_name => {configuration}.
  # Note - most BASIC configuration options are supported under the Event Hub names, and examples proved where applicable
  # Note - while in ADVANCED mode, if any BASIC options are defined at the top level they will be used if not already defined under the Event Hub name.  e.g. you may define shared configuration at the top level
  # Note - the required event_hub_connection is named 'event_hub_connection' (singular) which differs from the BASIC configuration option 'event_hub_connections' (plural)
  # Note - the 'event_hub_connection' may contain the 'EntityPath', but only if it matches the Event Hub name.
  # Example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #       }},
  #       { "event_hub_name2" => {
  #           event_hub_connection => "Endpoint=sb://example2..."
  #           storage_connection => "DefaultEndpointsProtocol=https;AccountName=example...."
  #           storage_container => "my_container"
  #      }}
  #    ]
  #    consumer_group => "logstash" # shared across all Event Hubs
  # }
  config :event_hubs, :validate => :array, :required => true # only required for advanced mode

  # BASIC MODE ONLY - The Event Hubs to read from. This is a list of Event Hub connection strings that includes the 'EntityPath'.
  # All other configuration options will be shared between Event Hubs.
  # Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"  , "Endpoint=sb://example2...;EntityPath=event_hub_name2"  ]
  # }
  config :event_hub_connections, :validate => :array, :required => true # only required for basic mode

  # Used to persists the offsets between restarts and ensure that multiple instances of Logstash process different partitions
  # This is *stongly* encouraged to be set for production environments.
  # When this value is set, restarts will pick up from where it left off. Without this value set the initial_position is *always* used.
  # BASIC Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    storage_connection => "DefaultEndpointsProtocol=https;AccountName=example...."
  # }
  # ADVANCED example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           storage_connection => "DefaultEndpointsProtocol=https;AccountName=example...."
  #       }}
  #    ]
  # }
  config :storage_connection, :validate => :password, :required => false

  # The storage container to persist the offsets.
  # Note - don't allow multiple Event Hubs to write to the same container with the same consumer group, else the offsets will be persisted incorrectly.
  # Note - this will default to the event hub name if not defined
  # BASIC Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    storage_connection => "DefaultEndpointsProtocol=https;AccountName=example...."
  #    storage_container => "my_container"
  # }
  # ADVANCED example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           storage_connection => "DefaultEndpointsProtocol=https;AccountName=example...."
  #           storage_container => "my_container"
  #       }}
  #    ]
  # }
  config :storage_container, :validate => :string, :required => false

  # Total threads used process events. Requires at minimum 2 threads. This option can not be set per Event Hub.
  # azure_event_hubs {
  #    threads => 4
  # }
  config :threads, :validate => :number, :default => 4

  # Consumer group used to read the Event Hub(s). It is recommended to change from the $Defualt to a consumer group specifically for Logstash, and ensure that all instances of Logstash use that consumer group.
  # BASIC Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    consumer_group => "logstash"
  # }
  # ADVANCED example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           consumer_group => "logstash"
  #       }}
  #    ]
  # }
  config :consumer_group, :validate => :string, :default => '$Default'

  # The max size of events are processed together. A checkpoint is created after each batch. Increasing this value may help with performance, but requires more memory.
  # Defaults to 50
  # BASIC Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    max_batch_size => 50
  # }
  # ADVANCED example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           max_batch_size => 50
  #       }}
  #    ]
  # }
  config :max_batch_size, :validate => :number, :default => 50

  # The max size of events that are retrieved prior to processing. Increasing this value may help with performance, but requires more memory.
  # Defaults to 300
  # BASIC Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    prefetch_count => 300
  # }
  # ADVANCED example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           prefetch_count => 300
  #       }}
  #    ]
  # }
  config :prefetch_count, :validate => :number, :default => 300

  # The max time allowed receive events without a timeout.
  # Value is expressed in seconds, default 60
  # BASIC Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    receive_timeout => 60
  # }
  # ADVANCED example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           receive_timeout => 300
  #       }}
  #    ]
  # }
  config :receive_timeout, :validate => :number, :default => 60

  # When first reading from an event hub, start from this position.
  # HEAD - reads ALL pre-existing events in the event hub
  # TAIL - reads NO pre-existing events in the event hub
  # LOOK_BACK - reads TAIL minus N seconds worth of pre-existing events
  # Note - If the storage_connection is set, this configuration is only applicable for the very first time Logstash reads from the event hub.
  # BASIC Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    initial_position => "HEAD"
  # }
  # ADVANCED example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           initial_position => "HEAD"
  #       }}
  #    ]
  # }
  config :initial_position, :validate => ['HEAD', 'TAIL', 'LOOK_BACK', 'head', 'tail', 'look_back'], :default => 'HEAD'

  # The number of seconds to look back for pre-existing events to determine the initial position.
  # Note - If the storage_connection is set, this configuration is only applicable for the very first time Logstash reads from the event hub.
  # Note - this options is only used when initial_position => "LOOK_BACK"
  # Value is expressed in seconds, default is 1 day
  # BASIC Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    initial_position => "LOOK_BACK"
  #    initial_position_look_back => 86400
  # }
  # ADVANCED example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           initial_position => "LOOK_BACK"
  #           initial_position_look_back => 86400
  #       }}
  #    ]
  # }
  config :initial_position_look_back, :validate => :number, :default => 86400

  # The interval in seconds between writing checkpoint while processing a batch. Default 5 seconds. Checkpoints can slow down processing, but are needed to know where to start after a restart.
  # Note - checkpoints happen after every batch, so this configuration is only applicable while processing a single batch.
  # Value is expressed in seconds, set to zero to disable
  # BASIC Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    checkpoint_interval => 5
  # }
  # ADVANCED example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           checkpoint_interval => 5
  #       }}
  #    ]
  # }
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
  # BASIC Example:
  # azure_event_hubs {
  #    config_mode => "BASIC"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    decorate_events => true
  # }
  # ADVANCED example:
  # azure_event_hubs {
  #   config_mode => "ADVANCED"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           decorate_events => true
  #       }}
  #    ]
  # }
  config :decorate_events, :validate => :boolean, :default => false


  def initialize(params)

    # explode the all of the parameters to be scoped per event_hub
    @event_hubs_exploded = []
    # global_config will be merged into the each of the exploded configs, prefer any configuration already scoped over the globally scoped config
    global_config = {}
    params.each do |k, v|
      if !k.eql?('id') && !k.eql?('event_hubs') && !k.eql?('threads') # don't copy these to the per-event-hub configs
        global_config[k] = v
      end
    end

    if params['config_mode'] && params['config_mode'].upcase.eql?('ADVANCED')
      params['event_hub_connections'] = ['dummy'] # trick the :required validation

      params['event_hubs'].each do |event_hub|
        raise "event_hubs must be a Hash" unless event_hub.is_a?(Hash)
        event_hub.each do |event_hub_name, config|
          config.each do |k, v|
            if 'event_hub_connection'.eql?(k) || 'storage_connection'.eql?(k) # protect from leaking logs
              config[k] = ::LogStash::Util::Password.new(v)
            end
          end
          if config['event_hub_connection'] #add the 's' to pass validation
            config['event_hub_connections'] = config['event_hub_connection']
            config.delete('event_hub_connection')
          end

          config.merge!({'event_hubs' => [event_hub_name]})
          config.merge!(global_config) {|k, v1, v2| v1}
          @event_hubs_exploded << config
        end
      end
    else # basic config
      params['event_hubs'] = ['dummy'] # trick the :required validation
      if params['event_hub_connections']
        params['event_hub_connections'].each do |connection|
          begin
            event_hub_name = ConnectionStringBuilder.new(connection).getEventHubName
            raise "invalid Event Hub name" unless event_hub_name
          rescue
            redacted_connection = connection.gsub(/(SharedAccessKey=)([0-9a-zA-Z=]*)([;]*)(.*)/, '\\1<redacted>\\3\\4')
            raise LogStash::ConfigurationError, "Error parsing event hub string name for connection: '#{redacted_connection}' please ensure that the connection string contains the EntityPath"
          end
          @event_hubs_exploded << {'event_hubs' => [event_hub_name]}.merge({'event_hub_connections' => [::LogStash::Util::Password.new(connection)]}).merge(global_config) {|k, v1, v2| v1}
        end
      end
    end

    super(params)

    container_consumer_groups = []
    # explicitly validate all the per event hub configs
    @event_hubs_exploded.each do |event_hub|
      if !self.class.validate(event_hub)
        raise LogStash::ConfigurationError, I18n.t("logstash.runner.configuration.invalid_plugin_settings")
      end
      container_consumer_groups << {event_hub['storage_connection'].value.to_s + (event_hub['storage_container'] ? event_hub['storage_container'] : event_hub['event_hubs'][0]) => event_hub['consumer_group']} if event_hub['storage_connection']
    end
    raise "The configuration will result in overwriting offsets. Please ensure that the each Event Hub's consumer_group is using a unique storage container." if container_consumer_groups.size > container_consumer_groups.uniq.size
  end

  attr_reader :event_hubs_exploded

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
                event_hub['event_hub_connections'].first.value, #there will only be one in this array by the time it gets here
                event_hub['storage_connection'].value,
                event_hub['storage_container'] ? event_hub['storage_container'] : event_hub_name,
                scheduled_executor_service)
          else
            @logger.warn("You have NOT specified a `storage_connection_string` for #{event_hub_name}. This configuration is only supported for a single Logstash instance.")
            checkpoint_manager = InMemoryCheckpointManager.new
            lease_manager = InMemoryLeaseManager.new
            event_processor_host = EventProcessorHost.new(
                EventProcessorHost.createHostName('logstash'),
                event_hub_name,
                event_hub['consumer_group'],
                event_hub['event_hub_connections'].first.value, #there will only be one in this array by the time it gets here
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


