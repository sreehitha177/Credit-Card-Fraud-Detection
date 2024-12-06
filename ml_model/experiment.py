from train import create_spark_session
from evaluate import load_model,load_test_data, evaluate_model


def main():
    # Path to the saved model and test data
    model_path = 'trained_model'  # Path where the model was saved
    test_data_path = 'testdata.csv'  # Path where the test data was saved

    # Create Spark session
    spark = create_spark_session()

    # Load the trained model
    model = load_model(model_path)

    # Load the test data
    test_data = load_test_data(test_data_path, spark)

    # Prepare the input columns used for training the model
    input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]

    # Evaluate the model using the test data
    auprc, precision, recall, f1_score = evaluate_model(model, test_data, input_cols)

if __name__ == "__main__":
    main()