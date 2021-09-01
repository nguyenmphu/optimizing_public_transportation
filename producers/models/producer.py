"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

from models.config import KAFKA_BOOTSTRAP_SERVER, KAFKA_SCHEMA_REGISTRY_URL

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.topic_name = topic_name

        self.broker_properties = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
            "schema.registry.url": KAFKA_SCHEMA_REGISTRY_URL,
        }

        # If the topic does not already exist, try to create it
        self.admin_client = AdminClient(
            conf={
                "bootstrap.servers": self.broker_properties["bootstrap.servers"]
            }
        )

        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            config=self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        topic = NewTopic(topic=self.topic_name,
                         num_partitions=self.num_partitions,
                         replication_factor=self.num_replicas)

        start = self.time_millis()
        self.admin_client.create_topics([topic])
        logger.info(f"Create topic: {self.topic_name}, run time: {self.time_millis() - start}ms")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.admin_client.delete_topics([self.topic_name])
