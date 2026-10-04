"""
This code is to demonstrate how consumer consumes the event/message from kafka
"""
from confluent_kafka import Consumer, KafkaError

# Setup and group co-ordination
conf = {
    'bootstrap.servers': 'localhost:9092', # kafka server
    'group.id': 'anaytics-group', # consumer group identity
    'auto.offset.reset': 'earliest', # earliest and latest when new consumer group identified by kafka
    'enable.auto.commit': True # automatically tell kafka  that successfully read the message every few seconds.
}

consumer = Consumer(conf)

# Subscribing to the topic/stream
consumer.subscribe(['user_signups'])

# Infinite poll loop "Engine"
try:
    while True:
        msg = consumer.poll(timeout=1.0) # Behind the scenes it sends "heartbeat" to kafka saying "I'm alive".
        # Safe error handling
        if msg is None:
            continue
        if msg.error():
            if msg.error().code == KafkaError._PARTITION_EOF:
                print(f"End of partition reached {msg.topic()}/{msg.partition()}")
            else:
                print(f"Error: {msg.error()}")
        else:
            # Data Serialization and processing
            print(f"Received message: key: {msg.key().decode("utf-8")}, value: {msg.value().decode("utf-8")}")
except KeyboardInterrupt:
    print(f"Stopped polling kafka for messages/events...!")
finally:
    # Clean shutdown
    consumer.close()