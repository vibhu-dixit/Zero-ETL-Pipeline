import os
import json
import time
from datetime import datetime, timedelta
import random
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "users"

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 2)
    )
    print(f"Kafka Producer connected to {KAFKA_BROKER}")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    exit(1)

def generate_user_data(user_id):
    """Generates a dictionary representing user data."""
    return {
        "id": str(user_id),
        "name": f"User_{user_id}",
        "email": f"user_{user_id}@example.com",
        "signup_ts": datetime.now().isoformat(timespec='milliseconds') + 'Z',
        "age": random.randint(18, 65)
    }

print(f"Starting to produce messages to topic: {TOPIC_NAME}")
user_id_counter = 1
while True:
    user_data = generate_user_data(user_id_counter)
    try:
        future = producer.send(TOPIC_NAME, value=user_data)
        record_metadata = future.get(timeout=10)
        print(f"Sent: {user_data['id']}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
        user_id_counter += 1
        time.sleep(1)
    except Exception as e:
        print(f"Error sending message: {e}")
        time.sleep(5)
    finally:
        producer.flush()