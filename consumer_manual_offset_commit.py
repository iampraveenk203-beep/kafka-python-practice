"""
To guarantee that a message is never skipped if your application crashes, you must turn off automatic tracking ('enable.auto.commit': False) and manually tell Kafka when a message is successfully processed.
asynchronous=True: It sends the request to Kafka and immediately continues to the next message without waiting for Kafka's reply. It is highly performant.
asynchronous=False: This blocks your code until Kafka acknowledges the write. Use this only during shutdown (inside finally) to make sure your very last offset is saved before the script dies.
"""
from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'critical-analytics-group',
    'auto.offset.reset': 'earliest',

    # Turn off automatic background commits
    'enable.auto.commit': False,
    'enable.auto.offset.store': True 
}

consumer = Consumer(conf)
consumer.subscribe(['user_signups'])

try:
    while True:
        msg = consumer.poll(timeout=1.0) # Blocking call that checks the kafka broker for new messages and waits for up to 1 sec if no data available.
        if msg is None:
            continue
        if msg.error():
            print(f"Error is: {msg.error()}")
            continue
        # Extract data
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")

        # Simulate processing(i.e Saving data to a database)
        try:
            print(f"Processing message/event: {key}")
            # db.save(value) <----- If this fails, code jumps to exception

            # Commit only after successfull processing of message
            # Asynchronous commit is fast and won't block the next loop
            consumer.commit(asynchronous=True)
        except Exception as e:
            # Do not commit here
            # On restart of consumer, it fetch this message/event again.
            print(f"Database operation failed for {key}: {e}")
except KeyboardInterrupt:
    print(f"Stopped polling kafka for messages/events on manual intervention...!")
finally:
    consumer.close()
