from kafka import KafkaConsumer, KafkaProducer
import os
import json
import numpy as np
import time

# Kafka configuration variables -- these are settings required to connect to Kafka.
KAFKA_BROKER_SERVICE = 'my-kafka.default.svc.cluster.local:9092'  # Address of the Kafka broker
KAFKA_CONSUMER_TOPIC = 'workers_topic'  # Topic from which to consume messages
KAFKA_PRODUCER_TOPIC = 'master'  # Topic to which to send the averaged vector
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME', 'user1')  # Username for Kafka authentication
KAFKA_PASSWORD = os.getenv('KAFKA_CONSUMER_PASSWORD')  # Password for Kafka authentication
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'master-group')  # Consumer group ID
KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest')  # Starting point for consumption


# Function to wait for workers to be ready
def wait_for_workers(consumer, num_workers):
    ready_workers = set()
    while len(ready_workers) < num_workers:
        records = consumer.poll(timeout_ms=10000)  # Adjust timeout as necessary
        for topic_partition, messages in records.items():
            for message in messages:
                worker_status = message.value

                # Check if the message is a dictionary with a 'status' key
                if isinstance(worker_status, dict) and 'status' in worker_status:
                    print("worker_status: ", worker_status)
                    if worker_status['status'] == 'ready':
                        ready_workers.add(worker_status['worker_id'])
                        print(f"Worker {worker_status['worker_id']} is ready.")

    print("All workers are ready.")

# Function to generate a vector of random weights
def generate_random_weights_vector(size=10):
    return np.random.rand(size).tolist()

# Function to send a vector of weights to the Kafka worker nodes topic with an end-of-iterations flag
def send_weights_to_workers(producer, weights_vector, message, end_of_iterations=False):
    payload = {'vector': weights_vector, 'end_of_iterations': end_of_iterations}
    producer.send(KAFKA_PRODUCER_TOPIC, value=json.dumps(payload).encode('utf-8'))
    producer.flush() # Ensure all messages are sent
    print(message)

def two_phase_process(producer, consumer, num_workers, timeslot_duration, num_iterations):
    # Wait for all workers to signal they are ready
    wait_for_workers(consumer, num_workers)

    # Phase 1: The master sends its random vector to workers, for initial random weights
    random_vector = generate_random_weights_vector()
    is_last_iteration = False
    send_weights_to_workers(producer, random_vector, "Sent weights to worker nodes.", end_of_iterations=is_last_iteration)

    # Phase 2: the learning loop
    for iteration in range(num_iterations):
        print(f"iteration {iteration + 1}/{num_iterations}")

        is_last_iteration = iteration == num_iterations - 1  # Correct placement

        # Collect and average vectors from workers
        vectors = {}
        start_time = time.time()
        while len(vectors) < num_workers and (time.time() - start_time) < timeslot_duration:
            records = consumer.poll(timeout_ms=timeslot_duration * 1000)
            for topic_partition, messages in records.items():
                for message in messages:
                    worker_id, vector = message.key.decode('utf-8'), message.value
                    if worker_id not in vectors:
                        vectors[worker_id] = vector
                        print(f"Received vector from worker {worker_id}: {vector}")

        # Compute and send back the average vector
        if len(vectors) == num_workers:
            average_vector = np.mean(list(vectors.values()), axis=0).tolist()
            send_weights_to_workers(producer, average_vector, "Sent average weights to worker nodes.", end_of_iterations=is_last_iteration)
            print(f"Average vector for iteration {iteration + 1}: {average_vector}")
        else:
            print("Did not receive vectors from all workers within the timeslot.")

        time.sleep(timeslot_duration)  # Wait before starting next iteration
  

if __name__ == "__main__":
    # Kafka producer setup for sending data back to the workers
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_SERVICE,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        value_serializer=lambda x: json.dumps(x).encode('utf-8') if not isinstance(x, bytes) else x
    )

    # Kafka consumer instantiation:
    consumer = KafkaConsumer(
        KAFKA_CONSUMER_TOPIC,
        bootstrap_servers=KAFKA_BROKER_SERVICE,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        group_id=KAFKA_CONSUMER_GROUP,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Parameters for two_phase_process
    num_workers = 3  # Expected number of vectors from workers
    timeslot_duration = 5  # Duration of each timeslot in seconds
    num_iterations = 5  # Number of two-phase iterations to perform (set to float('inf') for an indefinite loop)

    # Start the two-phase process with configured parameters
    two_phase_process(producer, consumer, num_workers, timeslot_duration, num_iterations)

    producer.close()
    consumer.close()
