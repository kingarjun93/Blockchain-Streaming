## Blockchain Streaming Analytics

This code was tested in Windows environment.

To start the zookeeper server execute the following command in command prompt.

`bin\windows\zookeeper-server-start ../../config/zookeeper.properties`

To start the Kafka server execute the following command in command prompt.

`bin\windows\kafka-server-start ../../config/server.properties`

#### Kafka_Producer_Script:
  This script contins code to start a kafka producer and send messages to a topic.
  It also writes the message to storage(as I had issues in setting up maven for spark kafka integration).
#### Count_to_redis:
  This script will read data with spark and write the count every minute to redis server
#### Kafka_Consumer_Script:
  This script will recieve messages from kafka stream and stores in a different path. hence demonstrating consumer-producer communication.
#### Flask-RESTAPI-Service:
  This script has REST API Exposed, so that API calls can be made with clients. Each endpoints are associated with a function that returns relevant values

Thanks for the opportunity.
