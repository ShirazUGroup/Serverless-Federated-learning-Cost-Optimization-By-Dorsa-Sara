from kafka import KafkaConsumer
import os
import base64
import re

# Kafka configuration variables
KAFKA_BROKER_SERVICE = 'my-kafka.kafka.svc.cluster.local:9092'  # DNS entry provided by Helm
KAFKA_TOPIC = 'test'  # Default testing topic as provided by Helm
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME', 'user1')  # Default to 'user1' if not set
KAFKA_PASSWORD = os.getenv('KAFKA_CONSUMER_PASSWORD')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'my-consumer-group')  # Consumer group ID
KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')  # Can be 'latest' or 'earliest'

def consume_messages_and_calculate_sum():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_SERVICE,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        group_id=KAFKA_CONSUMER_GROUP,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    total_rows_sum = 0
    for message in consumer:
        try:
            # Use a regular expression to find integers in the message value.
            number_of_rows = int(re.search(r'\d+', message.value).group())
            total_rows_sum += number_of_rows
            print(f"Received number of rows: {number_of_rows}, Current sum of rows: {total_rows_sum}")
        except ValueError as e:
            print(f"Could not convert message to int: {message.value}")
            print(f"Error: {e}")

if __name__ == "__main__":
    consume_messages_and_calculate_sum()
