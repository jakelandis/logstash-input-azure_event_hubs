package org.logstash;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    /**
     * Usage:
     * cd integration/event_hub_producer
     * mvn package
     * java -jar target/event-hub-producer.jar "[your_connection_string_here]"
     */
    public static void main(String... args) {


        if (args.length != 1 || !args[0].startsWith("Endpoint=sb") || !args[0].contains("EntityPath")) {
            LOGGER.error("The first and only argument must be the event hub connection string with the EntityPath. For example:");
            LOGGER.error("Endpoint=sb://logstash-demo.servicebus.windows.net/;SharedAccessKeyName=activity-log-ro;SharedAccessKey=<redacted>;EntityPath=my_event_hub");
            System.exit(1);
        }
        final int EVENTS_TO_SEND = 1000;
        LOGGER.info("Sending {} events ...", EVENTS_TO_SEND);
        try {
            final ExecutorService executorService = Executors.newSingleThreadExecutor();
            final EventHubClient client = EventHubClient.createSync(args[0], executorService);
            try {
                for (int i = 0; i < EVENTS_TO_SEND; i++) {
                    EventData sendEvent = EventData.create(Integer.toString(i).getBytes(StandardCharsets.UTF_8));
                    client.sendSync(sendEvent);
                }

                LOGGER.info("Successfully sent {} events to {}", EVENTS_TO_SEND, client.getEventHubName());

            } finally {
                try {
                    client.closeSync();
                    executorService.shutdown();
                } catch (Exception e) {
                    LOGGER.error("Exception while closing.", e);
                }
            }

        } catch (Throwable t) {
            LOGGER.error("Something bad just happened :(", t);
        }
    }

}
