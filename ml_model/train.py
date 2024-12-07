# train.py
# This file contains functions to train a machine learning model for credit card fraud detection using 
# Apache Spark. It includes functions to load, clean, and prepare data for training, as well as to train 
# and save a Random Forest Classifier model. The script orchestrates the end-to-end process of model 
# training, from data loading to saving the trained model and test data. Additionally, it provides 
# time tracking for each step to help evaluate performance.

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
import time

# Function to create a Spark session
def create_spark_session():
    """Initialize and return a Spark session."""
    return SparkSession.builder \
        .appName("CreditCardFraudDetection") \
        .master("local[*]").getOrCreate()

# Function to load dataset from a CSV file into a Spark DataFrame
def load_data(spark, file_path):
    """Load dataset from CSV."""
    start_time = time.time()  # Start timing
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    end_time = time.time()  # End timing
    
    print(f"Data loading time: {end_time - start_time} seconds")
    
    return data

# Function to clean data by removing rows with missing values
def clean_data(data):
    """Clean data by removing missing values."""
    start_time = time.time()  # Start timing
    data_cleaned = data.dropna()
    end_time = time.time()  # End timing
    
    print(f"Data cleaning time: {end_time - start_time} seconds")
    
    return data_cleaned

# Function to prepare feature columns for model training
def prepare_features(data):
    """Prepare features for training."""
    start_time = time.time()  # Start timing
    input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]
    end_time = time.time()  # End timing
    
    print(f"Feature preparation time: {end_time - start_time} seconds")
    
    return input_cols, data

# Function to train a Random Forest model using the prepared training data
def train_random_forest(train_data, input_cols):
    """Train Random Forest Classifier."""
    start_time = time.time()  # Start timing
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    processed_train_data = assembler.transform(train_data).select("features", "Class")

    rf = RandomForestClassifier(labelCol="Class", featuresCol="features", numTrees=100)
    model = rf.fit(processed_train_data)
    
    end_time = time.time()  # End timing
    print(f"Model training time: {end_time - start_time} seconds")
    
    return model

# Function to orchestrate the training process: load data, clean it, prepare features, and train the model
def train(file_path):
    """Orchestrate the training and evaluation process."""
    # Create Spark session
    spark = create_spark_session()

    # Load and clean data
    data = load_data(spark, file_path)
    data = clean_data(data)

    # Prepare features for training
    input_cols, processed_data = prepare_features(data)

    # Split data into train and test sets (80% training, 20% testing)
    train_data, test_data = processed_data.randomSplit([0.8, 0.2], seed=42)

    # Train the model
    model = train_random_forest(train_data, input_cols)

    return model, test_data

# Function to save the trained model to the specified path
def save_model(model, model_path):
    """Save the trained model to the given path."""
    model.save(model_path)
    print(f"Model saved to {model_path}")

# Function to save the test data to a specified Parquet path
def save_test_data(test_data, test_data_path):
    """Save the test data to the specified path in Parquet format."""
    test_data.write.mode("overwrite").parquet(test_data_path)
    print(f"Test data saved to {test_data_path}")

# Main function to train the model and save the trained model and test data
def main():
    """Main function to orchestrate the training, evaluation, and saving of the model and test data."""
    # Path to your credit card dataset
    file_path = 'creditcard.csv'  
    model_path = 'trained_model'  # Replace with the desired model save path

    # Train the model 
    model, test_data = train(file_path)
    
    # Save the model
    save_model(model, model_path)

    # Save test data 
    save_test_data(test_data, 'testdata.parquet')

if __name__ == "__main__":
    main()
