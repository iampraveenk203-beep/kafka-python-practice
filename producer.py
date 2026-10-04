"""
This code is to demonstrate the producer producing the event/message to kafka broker/server.
"""
# Import confluent_kafka (which is wrapper wrote aroung librdkafka (written in C))
from confluent_kafka import Producer
import time
import json

# Setup and Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

producer = Producer(conf)

# Delivery callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Defining message structure
topic = "user_signups"
key = "user_123"
value = '{"event": "signup", "timestamp": "2026-10-01"}'

# Sending the data
producer.produce(topic, key=key, value=value, callback=delivery_report)

# produced the message to kafka in the background by seperate internal thread, producer needs a way to pass the status back to your main thread Did it success? or fail ?
# Problem: Backround thread can't randomly interrupt your main python code to run the call back function "delivery_report"
# Solution: producer.poll(0) acts as a checkpoint. it tell producer: "Hey quickly check the backgrounf queue. successfully sent/failed just now, trigger thier call back function right here"
producer.poll(0) # (0) means non-blocking, it checks queue instantly, if there are call backs ready, it runs them, if not returns immediately in fraction of millisecond without making program wait.
time.sleep(0.5) # Best practice to pacing the stream, In real world production systems don't need sleep.
# Wait for any outstanding messages to be delivered
producer.flush()
print(f"Production complete ! Event stored in kafka.")