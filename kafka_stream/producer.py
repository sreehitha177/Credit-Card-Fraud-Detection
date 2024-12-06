from kafka import KafkaProducer
import json
import time

# Kafka Producer configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Simulate streaming transaction data
def generate_transaction_data():
    return {
        "V1": 0.1,  # Example feature V1
        "V2": 0.2,  # Example feature V2
        "Amount": 15.0,
        "Time": time.time(),
        "Class": 0  # 0 for non-fraudulent, 1 for fraudulent
    }

# Send data to Kafka topic every second
while True:
    data = generate_transaction_data()
    producer.send('test-topic', value=data)
    time.sleep(1)  # simulate real-time stream with a 1-second delay
