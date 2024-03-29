"""Create a new Pub/Sub topic."""
# [START pubsub_quickstart_create_topic]
# [START pubsub_create_topic]
from google.cloud import pubsub_v1


project_id = "csye-7200-team-4"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

topic = publisher.create_topic(request={"name": topic_path})

print(f"Created topic: {topic.name}")
# [END pubsub_quickstart_create_topic]
# [END pubsub_create_topic]