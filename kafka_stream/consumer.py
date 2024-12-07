# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, current_timestamp, unix_timestamp, avg, count,window
# from pyspark.sql.types import StructType, StructField, DoubleType
# from pyspark.ml.feature import VectorAssembler
# from pyspark.ml.classification import RandomForestClassificationModel
# import time

# # Schema definition
# schema = StructType([
#     StructField("Time", DoubleType(), True),
#     *[StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)],
#     StructField("Amount", DoubleType(), True),
#     StructField("Class", DoubleType(), True)
# ])

# input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]

# def main():
#     # Initialize Spark session
#     spark = SparkSession.builder \
#         .appName("FraudDetectionStreaming") \
#         .getOrCreate()

#     spark.sparkContext.setLogLevel("WARN")
#     # Load the trained model
#     model_path = "../ml_model/trained_model"  # Replace with your actual path
#     model = RandomForestClassificationModel.load(model_path)
#     print(f"Model loaded from: {model_path}")

#     # Read from Kafka
#     kafka_stream = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("subscribe", "task-topic") \
#         .load()

#     # Parse the Kafka stream
#     parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col("value"), schema).alias("data")) \
#         .select("data.*")

#     # Preprocess data
#     assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
#     processed_stream = assembler.transform(parsed_stream)

#     # Apply the trained model
#     predictions = model.transform(processed_stream)
#     predictions = predictions.withColumn("ProcessingTime", current_timestamp())
#     predictions = predictions.withColumn(
#         "ResponseTime", unix_timestamp(col("ProcessingTime")) - col("Time")
#     )

#     # Calculate transactions per second
#      # Calculate throughput
#     transactions_per_second = predictions.groupBy(
#         window(col("ProcessingTime"), "1 second")
#     ).agg(
#         count("*").alias("transactions_per_second")
#     )

#     # Calculate response time metrics
#     metrics = predictions.groupBy().agg(
#         count("*").alias("total_transactions"),
#         avg("ResponseTime").alias("avg_response_time")
#     )

#     # Write throughput to console
#     query_throughput = transactions_per_second.writeStream \
#         .outputMode("complete") \
#         .format("console") \
#         .start()

#     # Write response time metrics to console
#     query_metrics = metrics.writeStream \
#         .outputMode("complete") \
#         .format("console") \
#         .start()

    
    
#     # query_metrics.awaitTermination()


#     # Select flagged fraud alerts
#     fraud_alerts = predictions.filter(col("prediction") == 1)
#     query_alerts = fraud_alerts.selectExpr("to_json(struct(*)) AS value") \
#         .writeStream \
#         .format("console") \
#         .outputMode("append") \
#         .start()

#     print("Fraud detection streaming started...")
#     print("Streaming metrics monitoring started...")
    
#     query_alerts.awaitTermination()
#     query_throughput.awaitTermination()
#     query_metrics.awaitTermination()

# if __name__ == "__main__":
#     main()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, current_timestamp, unix_timestamp, avg, count, window
# from pyspark.sql.types import StructType, StructField, DoubleType
# from pyspark.ml.feature import VectorAssembler
# from pyspark.ml.classification import RandomForestClassificationModel
# import csv

# # Schema definition including IngestionRate
# schema = StructType([
#     StructField("Time", DoubleType(), True),
#     *[StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)],
#     StructField("Amount", DoubleType(), True),
#     StructField("Class", DoubleType(), True),
#     StructField("IngestionRate", DoubleType(), True)  # New field for ingestion rate
# ])

# input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]

# # File to save metrics
# metrics_file = 'streaming_metrics.csv'

# # Save metrics to a CSV file
# def save_metrics_to_csv(ingestion_rate, avg_throughput, avg_response_time):
#     with open(metrics_file, mode='a', newline='') as file:
#         writer = csv.writer(file)
#         writer.writerow([ingestion_rate, avg_throughput, avg_response_time])

# # Process each batch after model prediction and save metrics to CSV
# def process_batch(batch_df, batch_id):
#     if not batch_df.isEmpty():
#         # Extract ingestion rate from the batch (assuming it is the same for the entire batch)
#         ingestion_rate = batch_df.select("IngestionRate").first()["IngestionRate"]

#         # Calculate throughput (transactions per batch)
#         total_transactions = batch_df.count()
#         avg_throughput = total_transactions  # The number of transactions in the batch

#         # Calculate average response time for the batch
#         avg_response_time = batch_df.agg(avg("ResponseTime")).collect()[0][0]

#         # Save the metrics to CSV
#         save_metrics_to_csv(ingestion_rate, avg_throughput, avg_response_time)

# def main():
#     # Initialize Spark session
#     spark = SparkSession.builder \
#         .appName("FraudDetectionStreaming") \
#         .getOrCreate()

#     spark.sparkContext.setLogLevel("WARN")

#     # Load the trained model
#     model_path = "../ml_model/trained_model"  # Replace with your actual path
#     model = RandomForestClassificationModel.load(model_path)
#     print(f"Model loaded from: {model_path}")

#     # Read from Kafka
#     kafka_stream = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("subscribe", "task-topic") \
#         .load()

#     # Parse the Kafka stream
#     parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col("value"), schema).alias("data")) \
#         .select("data.*")

#     # Add a processing timestamp
#     parsed_stream = parsed_stream.withColumn("ProcessingTime", current_timestamp())

#     # Preprocess data
#     assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
#     processed_stream = assembler.transform(parsed_stream)

#     # Apply the trained model to make predictions
#     predictions = model.transform(processed_stream)

#     # Calculate the response time after the model has predicted
#     predictions = predictions.withColumn(
#         "ResponseTime", unix_timestamp(col("ProcessingTime")) - col("Time")
#     )

#     # Write metrics to CSV for each batch using foreachBatch
#     query_metrics = predictions.writeStream \
#         .outputMode("append") \
#         .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id)) \
#         .start()

#     # Select flagged fraud alerts and write to console
#     fraud_alerts = predictions.filter(col("prediction") == 1)
#     query_alerts = fraud_alerts.selectExpr("to_json(struct(*)) AS value") \
#         .writeStream \
#         .format("console") \
#         .outputMode("append") \
#         .start()

#     print("Fraud detection streaming started...")
#     print("Streaming metrics monitoring started...")

#     query_alerts.awaitTermination()
#     query_metrics.awaitTermination()

# if __name__ == "__main__":
#     # Write CSV header if file does not exist or is empty
#     with open(metrics_file, mode='w', newline='') as file:
#         writer = csv.writer(file)
#         writer.writerow(['ingestion_rate', 'avg_throughput', 'avg_response_time'])

#     # Run the consumer
#     main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unix_timestamp, avg, count,current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
import csv

metrics_file ='streaming_metrics.csv'

def main():
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define the schema for incoming Kafka data
    schema = StructType([
        StructField("Time", DoubleType(), True),
        *[StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)],
        StructField("Amount", DoubleType(), True),
        StructField("Class", DoubleType(), True),
        StructField("IngestionRate", DoubleType(), True)
    ])

    

    # Load the trained model
    model = RandomForestClassificationModel.load("../ml_model/trained_model")

    # Read from Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "task-topic") \
        .load()

    # Parse the Kafka stream
    parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Feature transformation
    assembler = VectorAssembler(inputCols=[f"V{i}" for i in range(1, 29)] + ["Amount"], outputCol="features")
    feature_data = assembler.transform(parsed_stream)

    # Apply the trained model
    predictions = model.transform(feature_data)

    # Add processing timestamp after model predictions
    predictions_with_time = predictions.withColumn("PostProcessingTime", current_timestamp())

    # Calculate response time
    predictions_with_time = predictions_with_time.withColumn(
        "ResponseTime", unix_timestamp("PostProcessingTime") - col("Time")
    )

    # Aggregate metrics
    metrics = predictions_with_time.groupBy("IngestionRate").agg(
        count("*").alias("total_transactions"),
        avg("ResponseTime").alias("avg_response_time")
    )

    # Write metrics to CSV
    def write_metrics_to_csv(batch_df, epoch_id):
        if not batch_df.isEmpty():
            batch_df = batch_df.collect()
            with open(metrics_file, 'a', newline='') as file:
                writer = csv.writer(file)
                for row in batch_df:
                    writer.writerow([row['IngestionRate'], row['total_transactions'], row['avg_response_time']])

    # Execute and await termination
    query = metrics.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_metrics_to_csv) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    # Initialize CSV file with headers
    with open(metrics_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['IngestionRate', 'TotalTransactions', 'AvgResponseTime'])
    main()
