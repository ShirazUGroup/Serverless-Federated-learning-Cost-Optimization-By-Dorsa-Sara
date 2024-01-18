from kafka import KafkaProducer
import pandas as pd
import os
import base64

# Kafka configuration variables
KAFKA_BROKER_SERVICE = 'my-kafka.kafka.svc.cluster.local:9092'  # DNS entry provided by Helm
KAFKA_TOPIC = 'test'  # Default testing topic as provided by Helm
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME', 'user1')  # Default to 'user1' if not set
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD_ENCODED')

# Producer function to send messages
def send_message(message):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_SERVICE,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        value_serializer=lambda m: str(m).encode('utf-8')
    )
    
    producer.send(KAFKA_TOPIC, message)
    producer.flush()

# Main function to read CSV and send a message
def main(csv_path):
    df = pd.read_csv(csv_path)
    message = f"The number of rows in the CSV file is: {len(df)}"
    send_message(message)

# Entry point
if __name__ == "__main__":
    main('/mnt/data_part2.csv')

