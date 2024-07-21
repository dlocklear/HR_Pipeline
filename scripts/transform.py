from pyspark.sql.functions import col, when, to_date, current_date


def transform_data(spark_df):
    # Cleaning Data
    spark_df = spark_df.dropna(subset=["Last", "First", "Position", "Hired", "Salary"])

    # Format date_hired column
    spark_df = spark_df.withColumn("Hired", to_date(col("Hired"), "MM/dd/yyyy"))

    # Calculate tenure
    spark_df = spark_df.withColumn(
        "tenure", (current_date() - col("Hired")).cast("int")
    )

    # Normalize titles
    spark_df = spark_df.withColumn(
        "Position",
        when(col("Position").rlike("(?i)manager"), "Manager")
        .when(col("Position").rlike("(?i)developer"), "Developer")
        .otherwise("Other"),
    )

    return spark_df
