from google.cloud import pubsub_v1
from faker import Faker
import json
import time
import random
project_id = "csye-7200-team-4"
topic_id = "my-topic"

# Initialize Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Initialize Faker for generating fake data
fake = Faker()

# Define a function to generate sample events using Faker and publish them to Pub/Sub
def generate_and_publish_events():
    while True:
        event_data = {
            "subscriberId": fake.uuid4(),
            "srcIP": fake.ipv4(),
            "dstIP": fake.ipv4(),
            "srcPort": random.randint(1024, 65535),
            "dstPort": random.randint(1024, 65535),
            "txBytes": random.randint(10000, 200000),
            "rxBytes": random.randint(10000, 50000),
            "startTime": int(time.time()),
            "endTime": int(time.time()),
            "tcpFlag": random.choice([0, 1, 2]),  # Random TCP flags
            "protocolName": random.choice(["tcp", "udp", "icmp"]),
            "protocolNumber": random.randint(0, 255),
        }

        # Introduce outlier data
        if random.random() < 0.05:  # 5% chance of
            # Example of outlier data (e.g., extremely high txBytes or rxBytes)
            event_data["txBytes"] = random.randint(500000, 1000000)
            event_data["rxBytes"] = random.randint(500000, 1000000)

        message_data = json.dumps(event_data)

        # Publish event data to Pub/Sub topic
        publisher.publish(topic_path, data=message_data.encode('utf-8'))
        print(f'Published message: {message_data}')

        time.sleep(random.randint(1, 5))  # Sleep for a random time between 1 to 5 seconds

# Call the function to start generating and publishing events
generate_and_publish_events()
