import os
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

# File paths
input_file = os.path.abspath("data/Employee_data.csv")
output_file = os.path.abspath("data/transformed_Employee_data.csv")


def main():
    # Extract
    print("Starting extraction...")
    spark_df = extract_data(input_file)
    print("Extraction completed.")

    # Show extracted data
    print("Extracted Data:")
    spark_df.show()

    # Transform
    print("Starting transformation...")
    transformed_df = transform_data(spark_df)
    print("Transformation completed.")

    # Show transformed data
    print("Transformed Data:")
    transformed_df.show()

    # Load
    print("Starting load...")
    load_data(transformed_df, output_file)
    print(f"Load completed. Data written to {output_file}")


if __name__ == "__main__":
    main()
