import pandas as pd
from pyspark.sql import SparkSession


def extract_data(file_path):
    # Initialize Spark session
    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

    # Load data from CSV using Pandas
    df = pd.read_csv(file_path)

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    return spark_df
