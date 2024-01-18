
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import numpy as np
import os
import json

# Kafka configuration variables
KAFKA_BROKER_SERVICE = 'my-kafka.default.svc.cluster.local:9092'
KAFKA_CONSUMER_TOPIC = 'master'  # Topic from which the worker consumes messages
KAFKA_PRODUCER_TOPIC = 'workers_topic'  # Topic to which the worker sends messages
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME', 'user1')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD_ENCODED')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'worker1-group')

# Function to signal that the worker is ready
def signal_ready(producer, worker_id):
    ready_signal = {'worker_id': worker_id, 'status': 'ready'}
    producer.send(KAFKA_PRODUCER_TOPIC, key=worker_id.encode('utf-8'), value=json.dumps(ready_signal).encode('utf-8'))
    producer.flush()

def preprocess_data(df):
    # Replace missing values (denoted as '?' in the dataset) with NaN and drop those rows
    df.replace("?", float("nan"), inplace=True)
    df.dropna(inplace=True)

    # Define the mapping of each attribute value to an integer
    class_map = {'no-recurrence-events':0, 'recurrence-events':1}
    age_map = {'10-19': 0, '20-29': 1, '30-39': 2, '40-49': 3, '50-59': 4, '60-69': 5, '70-79': 6, '80-89': 7, '90-99': 8}
    menopause_map = {'lt40': 0, 'ge40': 1, 'premeno': 2}
    tumor_size_map = {'0-4': 0, '5-9': 1, '10-14': 2, '15-19': 3, '20-24': 4, '25-29': 5, '30-34': 6, '35-39': 7, '40-44': 8, '45-49': 9, '50-54': 10}
    inv_nodes_map = {'0-2': 0, '3-5': 1, '6-8': 2, '9-11': 3, '12-14': 4, '15-17': 5, '18-20': 6, '21-23': 7, '24-26': 8, '27-29': 9, '30-32': 10, '33-35': 11, '36-39': 12, '40-44': 13, '45-49': 14, '50-54': 15, '55-59': 16}
    node_caps_map = {'yes': 0, 'no': 1}
    deg_malig_map = {1: 0, 2: 1, 3: 2}
    breast_map = {'left': 0, 'right': 1}
    breast_quad_map = {'left_up': 0, 'left_low': 1, 'right_up': 2, 'right_low': 3, 'central': 4}
    irradiat_map = {'yes': 0, 'no': 1}

    mapping = [
        class_map,
        age_map,
        menopause_map,
        tumor_size_map,
        inv_nodes_map,
        node_caps_map,
        deg_malig_map,
        breast_map,
        breast_quad_map,
        irradiat_map
    ]

    # Encoding the data
    for i in df.columns:
        df[i] = df[i].map(mapping[df.columns.get_loc(i)])

    return df

# Function to calculate weighted average
def calculate_weighted_average(df, vector):
    # Ensure vector length matches the number of DataFrame columns
    if len(vector) != df.shape[1]:
        raise ValueError("Length of the vector must be equal to the number of columns in the DataFrame")
    weighted_df = df.multiply(vector, axis=1)
    weighted_avgs = (weighted_df.sum(axis=0) / vector.sum()).values
    return weighted_avgs

# Function to send the calculated vector back to Kafka
def send_results(producer, vector, worker_id):
    producer.send(KAFKA_PRODUCER_TOPIC, key=worker_id.encode('utf-8'), value=json.dumps(vector.tolist()).encode('utf-8'))

# Main function
def main(csv_path, worker_id):
    df = pd.read_csv(csv_path, index_col=0)
    preprocessed_df = preprocess_data(df)
    
    # Kafka producer setup for sending data back to the master
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_SERVICE,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        value_serializer=lambda x: x if isinstance(x, bytes) else json.dumps(x).encode('utf-8')
    )

    # Kafka consumer setup for receiving weights vector from master
    consumer = KafkaConsumer(
        KAFKA_CONSUMER_TOPIC,
        bootstrap_servers=KAFKA_BROKER_SERVICE,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset='latest', # To consume only the latest messages
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    # Signal that this worker is ready
    signal_ready(producer, worker_id)

    # Process messages in a loop
    while True:
        for message in consumer:
            payload = message.value
            vector = np.array(payload['vector'])
            end_of_iterations = payload['end_of_iterations']

            if vector.size == len(preprocessed_df.columns):
                weighted_avgs = calculate_weighted_average(preprocessed_df, vector)
                send_results(producer, weighted_avgs, worker_id)
                print(f"Worker {worker_id}: The weighted average has been calculated and sent back to the master node.")

            if end_of_iterations:
                print(f"Worker {worker_id}: Final iteration completed. Exiting.")
                break

        if end_of_iterations:
            break

    consumer.close()
    producer.close()

if __name__ == "__main__":
    worker_id = 'worker1'  # Unique identifier for each worker
    main('/mnt/data_part1.csv', worker_id)
