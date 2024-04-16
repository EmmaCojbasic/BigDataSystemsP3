from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("ProcessCSV") \
        .getOrCreate()

    dir3_path = 'hdfs://namenode:9000/dir3'
    dir4_path = 'hdfs://namenode:9000/dir4'

    combined_df_1 = spark.read.csv(dir3_path, header=True, inferSchema=True)
    combined_df_2 = spark.read.csv(dir4_path, header=True, inferSchema=True)

    combined_df_1.write.mode('append').csv('hdfs://namenode:9000/results/results_emission.csv', header=True)
    combined_df_2.write.mode('append').csv('hdfs://namenode:9000/results/results_fcd.csv', header=True)

    spark.stop()

    print("CSV files saved successfully in HDFS.")

# if __name__ == "__main__":
#     # Initialize Spark session
#     spark = SparkSession.builder \
#         .appName("ProcessCSV") \
#         .getOrCreate()

#     # Directory paths on HDFS
#     dir3_path = 'hdfs://namenode:9000/dir3'
#     dir4_path = 'hdfs://namenode:9000/dir4'

#     # Read CSV files into Spark DataFrames
#     combined_df_1 = spark.read.csv(dir3_path, header=True, inferSchema=True)
#     combined_df_2 = spark.read.csv(dir4_path, header=True, inferSchema=True)

#     # Write DataFrames to local CSV files
#     combined_df_1.write.mode('overwrite').csv("C:/Users/Emma/Desktop/BDP3/BDSP3/results/results_emission.csv", header=True)
#     combined_df_2.write.mode('overwrite').csv("C:/Users/Emma/Desktop/BDP3/BDSP3/results/results_fcd.csv", header=True)

#     # Stop Spark session
#     spark.stop()

#     print("CSV files saved successfully locally.")

