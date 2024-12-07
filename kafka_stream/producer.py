# import time
# import json
# from kafka import KafkaProducer
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from pyspark.sql.types import DoubleType
# from pyspark.sql import functions as F
# # Kafka Producer configuration
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# def load_test_data(test_data_path, spark):
#     """Load test data from CSV using Spark."""
#     test_data = spark.read.option("header", "true").csv(test_data_path)
#     print(f"Test data loaded from {test_data_path}")
    
#     # Cast all required columns to DoubleType
#     columns_to_cast = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]
#     for col_name in columns_to_cast:
#         test_data = test_data.withColumn(col_name, col(col_name).cast(DoubleType()))
        
#     # Cast the 'Class' column to DoubleType as well
#     test_data = test_data.withColumn("Class", col("Class").cast(DoubleType()))
    
#     # Optional: Print the first few rows to verify
#     test_data.show(5)
    
#     return test_data

# # Generate transaction data from test data
# def generate_transaction_data(test_data):
#     """Randomly select a transaction from the test data."""
#     # Randomly sample one row from the test data
#     row = test_data.orderBy(F.rand()).limit(1).collect()[0]

#     # Convert the row to a dictionary
#     transaction = {col: row[col] for col in row.asDict().keys()}
    
#     # Add timestamp to simulate real-time
#     transaction["Time"] = time.time()
    
#     return transaction

# def main(rate,total_transactions):
#     # Initialize Spark session
#     spark = SparkSession.builder \
#         .appName("FraudDetectionStreaming") \
#         .getOrCreate()

#     # Load test data from CSV using Spark
#     file_path = "../ml_model/testdata.csv"  # Replace with your actual file path
#     test_data = load_test_data(file_path, spark)

# #     # Stream data to Kafka
#     topic = "task-topic"
#     print(f"Starting to stream data to Kafka topic: {topic}")

# #     while True:
# #         # Generate a single transaction
# #         transaction = generate_transaction_data(test_data)

# #         # Send transaction to Kafka
# #         producer.send(topic, value=transaction)
# #         print(f"Sent: {transaction}")

# #         # Simulate a 1-second delay between transactions
# #         time.sleep(1/rate)

# # if __name__ == "__main__":
# #     rate=1
# #     main(rate)
#     start_time = time.time()

#     for i in range(total_transactions):
#         transaction = generate_transaction_data(test_data)
#         producer.send(topic, value=transaction)
#         # print(f"Sent: {transaction}")
#         time.sleep(1 / rate)

#     end_time = time.time()
#     total_time = end_time - start_time

#     throughput = total_transactions / total_time
#     print(f"Throughput: {throughput:.2f} transactions/second")
#     print(f"Response Time per transaction: {total_time / total_transactions:.4f} seconds")

# if __name__ == "__main__":
#     rate = 10  # Transactions per second
#     total_transactions = 500
#     main(rate, total_transactions)
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
    # test_data.show(5)
    
    return test_data

# Generate transaction data from test data with ingestion rate
def generate_transaction_data(test_data, ingestion_rate):
    """Randomly select a transaction from the test data and add ingestion rate."""
    # Randomly sample one row from the test data
    row = test_data.orderBy(F.rand()).limit(1).collect()[0]

    # Convert the row to a dictionary
    transaction = {col: row[col] for col in row.asDict().keys()}

    # Add timestamp to simulate real-time ingestion
    transaction["Time"] = time.time()

    # Add ingestion rate to the transaction data
    transaction["IngestionRate"] = ingestion_rate

    return transaction

def main(rate, total_transactions):
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

    start_time = time.time()

    for i in range(total_transactions):
        transaction = generate_transaction_data(test_data, rate)
        producer.send(topic, value=transaction)
        # print(f"Sent: {transaction}")  # Optional: Log each transaction sent to Kafka
        time.sleep(1 / rate)

    end_time = time.time()
    total_time = end_time - start_time

    throughput = total_transactions / total_time
    print(f"Throughput: {throughput:.2f} transactions/second")
    print(f"Response Time per transaction: {total_time / total_transactions:.4f} seconds")

if __name__ == "__main__":
    # ingestion_rates = [10,25,50,75,100,150,200]  # Different ingestion rates to test (transactions per second)
    ingestion_rates = [10,25]
    total_transactions = 100

    for rate in ingestion_rates:
        print(f"Testing with ingestion rate: {rate} transactions/second")
        main(rate, total_transactions)
        time.sleep(10)  # Wait between tests for Kafka to handle data
