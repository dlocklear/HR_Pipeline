import os
import pandas as pd
import stat


def load_data(spark_df, output_path):
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = spark_df.toPandas()

    # Ensure the directory exists
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")

    # Remove the file if it already exists
    if os.path.exists(output_path):
        try:
            print(f"Removing existing file: {output_path}")
            # Ensure the file is writable
            os.chmod(output_path, stat.S_IWRITE)
            os.remove(output_path)
        except PermissionError:
            print(f"PermissionError: Unable to remove file: {output_path}")
            print("Attempting to change permissions and retry...")
            try:
                os.chmod(output_path, stat.S_IWRITE)
                os.remove(output_path)
            except Exception as e:
                print(
                    f"Failed to remove file {output_path} after changing permissions: {e}"
                )
                return

    # Write Pandas DataFrame to a new CSV file
    print(f"Writing to file: {output_path}")
    pandas_df.to_csv(output_path, index=False)
    print(f"Data written to {output_path}")
