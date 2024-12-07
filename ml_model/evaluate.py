# evaluate.py
# This file contains functions to evaluate a trained machine learning model on a given test dataset. 
# It includes functions to load the trained model, load and preprocess the test data, evaluate the model 
# incrementally on subsets of the test data, and print evaluation metrics (AUPRC, precision, recall, F1-score).

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from train import create_spark_session
import time

# Function to load a pre-trained Random Forest model from the specified path
def load_model(model_path):
    """Load the trained model from the given path."""
    model = RandomForestClassificationModel.load(model_path)
    print(f"Model loaded from {model_path}")
    return model

# Function to load the test data from a Parquet file and cast columns to the correct type
def load_test_data(test_data_path, spark):
    """Load the test data from Parquet."""
    test_data = spark.read.parquet(test_data_path)
    print(f"Test data loaded from {test_data_path}")
    # Convert columns to DoubleType if not already cast
    columns_to_cast = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount", "Class"]
    for col_name in columns_to_cast:
        test_data = test_data.withColumn(col_name, col(col_name).cast(DoubleType()))
    return test_data

# Function to evaluate the model incrementally on subsets of the test data
def evaluate_incrementally(model, test_data, input_cols, size_list):
    """Evaluate the model incrementally with larger datasets."""
    for subset_size in size_list:
        subset = test_data.limit(subset_size)
        print(f"Evaluating on dataset with {subset.count()} records...")  # Add here
        
        # Evaluate the model on the subset
        auprc, precision, recall, f1_score = evaluate_model(model, subset, input_cols)
        
        # Print metrics for the current subset
        print(f"Results for subset size {subset_size}:")
        print(f"- AUPRC: {auprc:.4f}")
        print(f"- Precision: {precision:.4f}")
        print(f"- Recall: {recall:.4f}")
        print(f"- F1-Score: {f1_score:.4f}")

# Function to evaluate the trained model on the test data and calculate various metrics
def evaluate_model(model, test_data, input_cols):
    """Evaluate the model on the test data."""
    # Prepare test data by transforming features into a vector column
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    processed_test_data = assembler.transform(test_data).select("features", "Class")

    # Make predictions on the test data
    predictions = model.transform(processed_test_data)

    # AUPRC (Area Under Precision-Recall Curve)
    evaluator = BinaryClassificationEvaluator(labelCol="Class", metricName="areaUnderPR")
    auprc = evaluator.evaluate(predictions)

    # Precision, Recall, and F1-Score (Weighted)
    evaluator_precision = MulticlassClassificationEvaluator(labelCol="Class", metricName="weightedPrecision")
    precision = evaluator_precision.evaluate(predictions)

    evaluator_recall = MulticlassClassificationEvaluator(labelCol="Class", metricName="weightedRecall")
    recall = evaluator_recall.evaluate(predictions)

    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="Class", metricName="f1")
    f1_score = evaluator_f1.evaluate(predictions)

    # Print out the evaluation metrics
    print(f"AUPRC: {auprc}")
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"F1-Score: {f1_score}")

    return auprc, precision, recall, f1_score