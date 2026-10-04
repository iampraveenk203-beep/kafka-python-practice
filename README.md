# kafka-python-practice
This repository is to demonstrate a the confluent_kafka (written in C as librdkafka).
Explains below components:
Producer.
Consumer.
Topic.
Partition.
poll.
sleep.

# 1. Download the actual, direct functional Kafka zip file
# Get valid wget command to download kafka
wget https://apache.org 

# 2. Extract the file structure
tar -xzf kafka_2.13-3.6.0.tgz

# 3. Enter the extracted folder
cd kafka_2.13-3.6.0

# 4. Start Zookeeper cleanly in the background
bin/zookeeper-server-start.sh config/zookeeper.properties > zookeeper.log 2>&1 &

# 5. Start Kafka cleanly in the background
bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &