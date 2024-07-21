def load_data(spark_df, output_path):
    # Write transformed data to a new CSV file
    spark_df.write.csv(output_path, header=True, mode="overwrite")
    print(f"Data written to {output_path}")
    
    # Uncomment and modify the following lines to write to a database
    # from pyspark.sql import SaveMode
    # spark_df.write.format('jdbc').options(
    #     url='jdbc:mysql://localhost:3306/employees',
    #     driver='com.mysql.cj.jdbc.Driver',
    #     dbtable='employee_data',
    #     user='root',
    #     password='password'
    # ).mode(SaveMode.Append).save()
