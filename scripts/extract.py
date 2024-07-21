import pandas as pd
from pyspark.sql import SparkSession

def extract_data(file_path):
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("ETL Pipeline")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.hadoop.fs.local.block.size", "67108864")
        .config("spark.hadoop.io.file.buffer.size", "4096")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .getOrCreate()
    )

    # Print Spark configurations
    print("Spark configurations:")
    for item in spark.sparkContext.getConf().getAll():
        print(item)

    # Load data from CSV using Pandas
    df = pd.read_csv(file_path)

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    return spark_df
