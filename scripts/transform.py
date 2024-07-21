from pyspark.sql.functions import col, when, to_date, current_date


def transform_data(spark_df):
    # Cleaning Data
    spark_df = spark_df.dropna(subset=["name", "title", "date_hired", "salary"])

    # Format date_hired column
    spark_df = spark_df.withColumn(
        "date_hired", to_date(col("date_hired"), "MM/dd/yyyy")
    )

    # Calculate tenure
    spark_df = spark_df.withColumn(
        "tenure", (current_date() - col("date_hired")).cast("int")
    )

    # Normalize titles
    spark_df = spark_df.withColumn(
        "title",
        when(col("title").rlike("(?i)manager"), "Manager")
        .when(col("title").rlike("(?i)developer"), "Developer")
        .otherwise("Other"),
    )

    return spark_df
