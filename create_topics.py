from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
)

kkey = "_vp2"
try:
    for topic_name in ["building_sensors" + kkey, "temperature_alerts" + kkey, "humidity_alerts" + kkey]:
        new_topic = NewTopic(name=topic_name, num_partitions=2, replication_factor=1)
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

[print(topic) for topic in admin_client.list_topics() if "my_name" in topic]

admin_client.close()
