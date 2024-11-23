from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    "building_sensors_vp2",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    group_id="alert_processor_group",
    auto_offset_reset='earliest'
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
)

for message in consumer:
    data = message.value
    alerts = []

    if data["temperature"] > 40:
        alerts.append({
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "value": data["temperature"],
            "message": "Temperature exceeds threshold!"
        })

    if data["humidity"] < 20 or data["humidity"] > 80:
        alerts.append({
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "value": data["humidity"],
            "message": "Humidity out of range!"
        })

    for alert in alerts:
        topic_name = "temperature_alerts_vp2" if "Temperature" in alert["message"] else "humidity_alerts_vp2"
        producer.send(topic_name, value=alert)
        print(f"Sent alert to {topic_name}: {alert}")
