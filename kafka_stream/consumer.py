from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel




# Dynamically generate StructField for V1 to V28
schema = StructType([
    StructField("Time", DoubleType(), True),
    *[
        StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)
    ],
    StructField("Amount", DoubleType(), True),
    StructField("Class", DoubleType(), True)
])


# Input columns for the model
input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .getOrCreate()

    # Load the trained model
    model_path = "../ml_model/trained_model"  # Replace with your actual model path
    model = RandomForestClassificationModel.load(model_path)
    print(f"Trained model loaded from: {model_path}")

    # Read from Kafka #TODO: PUT A GOOD TOPIC NAME
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "task-topic") \
        .load()

    # Parse the Kafka stream
    parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Preprocess data
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    processed_stream = assembler.transform(parsed_stream)

    # Apply the trained model
    predictions = model.transform(processed_stream)

    # Select flagged fraud alerts
    fraud_alerts = predictions.filter(col("prediction") == 1)

    # Write flagged fraud alerts to another Kafka topic
    query = fraud_alerts.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "fraud-alerts") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark/checkpoints") \
        .start()

    print("Fraud detection streaming started...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
