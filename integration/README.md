Integration Testing
--------------

Integration testing is currently a manual effort. An Event Hub producer and consumer are provided to help with that manual effort. 

## Producer

To write 1000 events to an Event Hub:
```bash
cd integration/event_hub_producer
mvn package
java -jar target/event-hub-producer.jar "[your_connection_string_here]"
```  
Note - you connection string must contain the EntityPath 

## Consumer

To read 1000 events from an Event Hub:
```bash
cd integration/event_hub_consumer
mvn package
java -jar target/event-hub-consumer.jar "[your_connection_string_here]"
```  
Note - you connection string must contain the EntityPath 