# Blockchain-Streaming

1. Kafka_Producer_Script:
  This script contins code to start a kafka producer and send messages to a topic.
  It also writes the message to storage(as I had issues in setting up maven for spark kafka integration).
2. Count_to_redis:
  This script will read data with spark and write the count every minute to redis server
3. Kafka_Consumer_Script:
  This script will recieve messages from kafka stream and stores in a different path. hence demonstrating consumer.
4. Flask-RESTAPI-Service:
  This script has REST API Exposed, so that API calls can be made with clients.(logic to give out actual results were not completed)

Thanks for the opportunity.
