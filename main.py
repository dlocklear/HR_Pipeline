from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

# File paths
input_file = "data/employee_data.csv"
output_file = "data/transformed_employee_data.csv"


def main():
    # Extract
    spark_df = extract_data(input_file)

    # Transform
    transformed_df = transform_data(spark_df)

    # Load
    load_data(transformed_df, output_file)


if __name__ == "__main__":
    main()
