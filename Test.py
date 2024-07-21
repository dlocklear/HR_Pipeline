import pandas as pd
import glob

# List all files in the transformed data directory
all_files = glob.glob("data/transformed_Employee_data.csv/part-*.csv")

# Check if there are any files to read
if all_files:
    df_list = [pd.read_csv(file) for file in all_files]
    transformed_data = pd.concat(df_list, ignore_index=True)
    # Display the DataFrame
    print(transformed_data.head())
else:
    print("No files found in the directory.")
