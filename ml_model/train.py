# train.py

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
# from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
# from pyspark.sql.functions import col

def create_spark_session():
    """Initialize and return Spark session."""
    return SparkSession.builder \
        .appName("CreditCardFraudDetection") \
        .getOrCreate()

def load_data(spark, file_path):
    """Load dataset from CSV."""
    return spark.read.csv(file_path, header=True, inferSchema=True)

def clean_data(data):
    """Clean data by removing missing values."""
    return data.dropna()

def prepare_features(data):
    """Prepare features for training."""
    input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]
    return input_cols, data

def train_random_forest(train_data, input_cols):
    """Train Random Forest Classifier."""
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    processed_train_data = assembler.transform(train_data).select("features", "Class")

    rf = RandomForestClassifier(labelCol="Class", featuresCol="features", numTrees=100)
    model = rf.fit(processed_train_data)
    
    return model

def train(file_path):
    """Orchestrate the training and evaluation process."""
    # Create Spark session
    spark = create_spark_session()

    # Load and clean data
    data = load_data(spark, file_path)
    data = clean_data(data)

    # Prepare features for training
    input_cols, processed_data = prepare_features(data)

    # Split data into train and test
    train_data, test_data = processed_data.randomSplit([0.8, 0.2], seed=42)

    # Train the model
    model = train_random_forest(train_data, input_cols)

    return model,test_data

def save_model(model, model_path):
    """Save the trained model to the given path."""
    model.save(model_path)
    print(f"Model saved to {model_path}")

def save_test_data(test_data, test_data_path):
    """Save the test data to the specified path in CSV format."""
    test_data.write.option("header", "true").csv(test_data_path)
    print(f"Test data saved to {test_data_path}")


def main():
    # Path to your credit card dataset
    file_path = 'creditcard.csv'  
    model_path = 'trained_model'  # Replace with the desired model save path

    # Train the model 
    model,test_data = train(file_path)
    
    # Save the model
    save_model(model,model_path)

    # Save test_data 
    save_test_data(test_data,'testdata.csv')



if __name__ == "__main__":
    main()

