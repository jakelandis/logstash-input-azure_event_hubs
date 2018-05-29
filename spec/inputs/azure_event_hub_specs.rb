# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/azure_event_hubs"

describe LogStash::Inputs::AzureEventHubs do

  subject(:input) {LogStash::Plugin.lookup("input", "azure_event_hubs").new(config)}

  describe "Event Hubs Configuration -> " do
    shared_examples "an exploded Event Hub config" do |x|
      it "it explodes #{x} event hub(s) correctly" do
        exploded_config = input.event_hubs_exploded
        x.times do |i|
          expect(exploded_config[i]['event_hubs'].size).to be == 1 #always 1 in the exploded form
          expect(exploded_config[i]['event_hubs'][0]).to eql('event_hub_name' + i.to_s)
          expect(exploded_config[i]['event_hub_connections'][0].value).to start_with('Endpoint=sb://...')
          expect(exploded_config[i]['storage_connection'].value).to eql('DefaultEndpointsProtocol=https;AccountName=...')
          expect(exploded_config[i]['threads']).to be_nil # we don't explode threads to the per event hub config
          expect(exploded_config[i]['codec']).to be_a_kind_of(LogStash::Codecs::Plain)
          expect(exploded_config[i]['consumer_group']).to eql('cg')
          expect(exploded_config[i]['max_batch_size']).to be == 20
          expect(exploded_config[i]['prefetch_count']).to be == 30
          expect(exploded_config[i]['receive_timeout']).to be == 40
          expect(exploded_config[i]['initial_position']).to eql('LOOK_BACK')
          expect(exploded_config[i]['initial_position_look_back']).to be == 50
          expect(exploded_config[i]['checkpoint_interval']).to be == 60
          expect(exploded_config[i]['decorate_events']).to be_truthy
        end
      end
    end

    describe "Basic Config" do
      let(:config) do
        {
            'event_hub_connections' => ['Endpoint=sb://...;EntityPath=event_hub_name0', 'Endpoint=sb://...;EntityPath=event_hub_name1'],
            'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...',
            'threads' => 8,
            'codec' => 'plain',
            'consumer_group' => 'cg',
            'max_batch_size' => 20,
            'prefetch_count' => 30,
            'receive_timeout' => 40,
            'initial_position' => 'LOOK_BACK',
            'initial_position_look_back' => 50,
            'checkpoint_interval' => 60,
            'decorate_events' => true
        }
      end
      it_behaves_like "an exploded Event Hub config", 2
    end

    describe "Advanced Config" do
      let(:config) do
        {
            'config_mode' => 'ADVANCED',
            'event_hubs' => [
                'event_hub_name0' => {
                    'event_hub_connection' => 'Endpoint=sb://...',
                    'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...',
                    'codec' => 'plain',
                    'consumer_group' => 'cg',
                    'max_batch_size' => 20,
                    'prefetch_count' => 30,
                    'receive_timeout' => 40,
                    'initial_position' => 'LOOK_BACK',
                    'initial_position_look_back' => 50,
                    'checkpoint_interval' => 60,
                    'decorate_events' => true},
                'event_hub_name1' => {
                    'event_hub_connection' => '1Endpoint=sb://...',
                    'storage_connection' => '1DefaultEndpointsProtocol=https;AccountName=...',
                    'codec' => 'json',
                    'consumer_group' => '1cg',
                    'receive_timeout' => 41,
                    'initial_position' => 'tail',
                    'checkpoint_interval' => 61,
                    'decorate_events' => false}
            ],
            'codec' => 'plain',
            'threads' => 8,
            'consumer_group' => 'default_consumer_group',
            'max_batch_size' => 21
        }
      end
      it_behaves_like "an exploded Event Hub config", 1
      it "it explodes the 2cnd advanced config event hub correctly" do
        exploded_config = input.event_hubs_exploded
        expect(exploded_config[1]['event_hubs'].size).to be == 1 #always 1 in the exploded form
        expect(exploded_config[1]['event_hubs'][0]).to eql('event_hub_name1')
        expect(exploded_config[1]['event_hub_connections'][0].value).to eql('1Endpoint=sb://...')
        expect(exploded_config[1]['storage_connection'].value).to eql('1DefaultEndpointsProtocol=https;AccountName=...')
        expect(exploded_config[1]['threads']).to be_nil # we don't explode threads to the per event hub config
        expect(exploded_config[1]['codec']).to be_a_kind_of(LogStash::Codecs::JSON) # different between configs
        expect(exploded_config[1]['consumer_group']).to eql('1cg') # override global
        expect(exploded_config[1]['max_batch_size']).to be == 21 # filled from global
        expect(exploded_config[1]['prefetch_count']).to be_nil # removed this from the config above
        expect(exploded_config[1]['receive_timeout']).to be == 41
        expect(exploded_config[1]['initial_position'].upcase).to eql('TAIL')
        expect(exploded_config[1]['initial_position_look_back']).to be_nil # removed this from the config above
        expect(exploded_config[1]['checkpoint_interval']).to be == 61
        expect(exploded_config[1]['decorate_events']).to be_falsy
      end
    end

    describe "Bad Basic Config" do
      describe "Offset overwritting" do
        let(:config) do
          {
              'event_hub_connections' => ['Endpoint=sb://...;EntityPath=event_hub_name0', 'Endpoint=sb://...;EntityPath=event_hub_name0'],
              'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...'
          }
        end
        it "it errors when using same consumer group and storage container" do
          expect {input}.to raise_error(/overwriting offsets/)
        end
      end

      describe "Invalid Event Hub name" do
        let(:config) do
          {
              'event_hub_connections' => ['Endpoint=sb://logstash/;SharedAccessKeyName=activity-log-readonly;SharedAccessKey=thisshouldnotbepartofthelogmessage'],
              'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...'
          }
        end
        it "it errors when using same consumer group and storage container" do
          expect {input}.to raise_error(/that the connection string contains the EntityPath/)
          expect {input}.to raise_error(/redacted/)
          expect {input}.to raise_error(/^((?!thisshouldnotbepartofthelogmessage).)*$/)
        end
      end

    end

    describe "Bad Advanced Config" do
      describe "Offset overwritting" do
        let(:config) do
          {
              'config_mode' => 'ADVANCED',
              'event_hubs' => [
                  'event_hub_name0' => {
                      'event_hub_connection' => 'Endpoint=sb://...',
                  },
                  'event_hub_name1' => {
                      'event_hub_connection' => '1Endpoint=sb://...',
                  }
              ],

              'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...',
              'consumer_group' => 'default_consumer_group',
              'storage_container' => 'logstash'
          }
        end
        it "it errors when using same consumer group and storage container" do
          expect {input}.to raise_error(/overwriting offsets/)
        end
      end
    end
  end
end

