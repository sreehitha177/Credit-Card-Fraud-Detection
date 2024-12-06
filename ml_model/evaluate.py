# evaluate.py

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

def load_model(model_path):
    """Load the trained model from the given path."""
    model = RandomForestClassificationModel.load(model_path)
    print(f"Model loaded from {model_path}")
    return model

def load_test_data(test_data_path, spark):
    """Load the test data from CSV."""
    test_data = spark.read.option("header", "true").csv(test_data_path)
    print(f"Test data loaded from {test_data_path}")

    columns_to_cast = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]
    for col_name in columns_to_cast:
        test_data = test_data.withColumn(col_name, col(col_name).cast(DoubleType()))
        
    # Cast the 'Class' column to DoubleType as well (target variable)
    test_data = test_data.withColumn("Class", col("Class").cast(DoubleType()))
    # print(test_data.show())
    return test_data
    # return test_data

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


