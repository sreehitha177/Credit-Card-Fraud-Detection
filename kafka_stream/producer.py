# from kafka import KafkaProducer
# import json
# import time

# # Kafka Producer configuration
# producer = KafkaProducer(bootstrap_servers='localhost:9092', 
#                           value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# # Simulate streaming transaction data
# def generate_transaction_data():
#     return {
#         "V1": 0.1,  # Example feature V1
#         "V2": 0.2,  # Example feature V2
#         "Amount": 15.0,
#         "Time": time.time(),
#         "Class": 0  # 0 for non-fraudulent, 1 for fraudulent
#     }

# # Send data to Kafka topic every second
# while True:
#     data = generate_transaction_data()
#     producer.send('task-topic', value=data)
#     time.sleep(1)  # simulate real-time stream with a 1-second delay

import time
import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
# Kafka Producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def load_test_data(test_data_path, spark):
    """Load test data from CSV using Spark."""
    test_data = spark.read.option("header", "true").csv(test_data_path)
    print(f"Test data loaded from {test_data_path}")
    
    # Cast all required columns to DoubleType
    columns_to_cast = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]
    for col_name in columns_to_cast:
        test_data = test_data.withColumn(col_name, col(col_name).cast(DoubleType()))
        
    # Cast the 'Class' column to DoubleType as well
    test_data = test_data.withColumn("Class", col("Class").cast(DoubleType()))
    
    # Optional: Print the first few rows to verify
    test_data.show(5)
    
    return test_data

# Generate transaction data from test data
def generate_transaction_data(test_data):
    """Randomly select a transaction from the test data."""
    # Randomly sample one row from the test data
    row = test_data.orderBy(F.rand()).limit(1).collect()[0]

    # Convert the row to a dictionary
    transaction = {col: row[col] for col in row.asDict().keys()}
    
    # Add timestamp to simulate real-time
    transaction["Time"] = time.time()
    
    return transaction

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .getOrCreate()

    # Load test data from CSV using Spark
    file_path = "../ml_model/testdata.csv"  # Replace with your actual file path
    test_data = load_test_data(file_path, spark)

    # Stream data to Kafka
    topic = "task-topic"
    print(f"Starting to stream data to Kafka topic: {topic}")

    while True:
        # Generate a single transaction
        transaction = generate_transaction_data(test_data)

        # Send transaction to Kafka
        producer.send(topic, value=transaction)
        print(f"Sent: {transaction}")

        # Simulate a 1-second delay between transactions
        time.sleep(1)

if __name__ == "__main__":
    main()
