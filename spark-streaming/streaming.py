import os
#import geopandas as gpd
# import matplotlib.pyplot as plt
# import geoplot as gplt
# import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructField, StructType, StringType, FloatType, DoubleType, IntegerType

if __name__ == "__main__":

    kafka_url = os.getenv('KAFKA_URL')
    fcd_topic = 'stockholm-fcd'
    emission_topic = 'stockholm-emission'
    
    window_duration = os.getenv('WINDOW_DURATION')
    REGRESSION_MODEL_LOCATION = os.getenv('REGRESSION_MODEL_LOCATION')
    CLUSTER_MODEL_LOCATION = os.getenv('CLUSTER_MODEL_LOCATION')

    vehicleSchema = StructType([
        StructField("timestep_time", FloatType()),
        StructField("vehicle_angle", FloatType()),
        StructField("vehicle_id", IntegerType()),
        StructField("vehicle_lane", StringType()),
        StructField("vehicle_pos", FloatType()),
        StructField("vehicle_slope", FloatType()),
        StructField("vehicle_speed", FloatType()),
        StructField("vehicle_type", StringType()),
        StructField("vehicle_x", FloatType()),
        StructField("vehicle_y", FloatType())
    ])

    emissionSchema = StructType([
        StructField("timestep_time", FloatType()),
        StructField("vehicle_CO", FloatType()),
        StructField("vehicle_CO2", FloatType()),
        StructField("vehicle_HC", FloatType()),
        StructField("vehicle_NOx", FloatType()),
        StructField("vehicle_PMx", FloatType()),
        StructField("vehicle_angle", FloatType()),
        StructField("vehicle_eclass", StringType()),
        StructField("vehicle_electricity", FloatType()),
        StructField("vehicle_id", IntegerType()),
        StructField("vehicle_lane", StringType()),
        StructField("vehicle_fuel", FloatType()),
        StructField("vehicle_noise", FloatType()),
        StructField("vehicle_pos", FloatType()),
        StructField("vehicle_route", StringType()),
        StructField("vehicle_speed", FloatType()),
        StructField("vehicle_type", StringType()),
        StructField("vehicle_waiting", FloatType()),
        StructField("vehicle_x", FloatType()),
        StructField("vehicle_y", FloatType())
    ])

    appName = "StreamingApp"

    conf = SparkConf()
    spark = SparkSession.builder.config(conf=conf).appName(appName).master("spark://spark:7077").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    cluster_model = PipelineModel.load('hdfs://namenode:9000/dir2/cmodel')
    regression_model = PipelineModel.load('hdfs://namenode:9000/dir2/rmodel')

    dfEmission = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_url) \
        .option("subscribe", emission_topic) \
        .load()
    
    dfEmission.printSchema()
    
    dfFcd = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_url) \
        .option("subscribe", fcd_topic) \
        .load()
    
    dfFcd.printSchema()

    # DESERIJALIZACIJA !!!!!!!!!!!!!!!!!!!!

    dfEmission = dfEmission.selectExpr("timestamp as event_time", "CAST(value AS STRING)")
    dfEmission = dfEmission.withColumn("value", from_json(dfEmission["value"], schema=emissionSchema))
    dfEmission = dfEmission.selectExpr(
        "value.vehicle_fuel as vehicle_fuel",
        "value.vehicle_speed as vehicle_speed",
        "value.vehicle_noise as vehicle_noise",
        "value.vehicle_NOx as vehicle_NOx"
    )

    dfFcd = dfFcd.selectExpr("timestamp as event_time", "CAST(value AS STRING)")
    dfFcd = dfFcd.withColumn("value", from_json(dfFcd["value"], schema=vehicleSchema))
    dfFcd = dfFcd.selectExpr(
        "value.vehicle_x as vehicle_x",
        "value.vehicle_y as vehicle_y",
    )

    clustered_df = cluster_model.transform(dfFcd)
    predicted_df = regression_model.transform(dfEmission)

    output_path_clustered = 'hdfs://namenode:9000/dir3'
    output_path_predicted = 'hdfs://namenode:9000/dir4'

    # OTPAKOVANJE !!!!!!!!!!!!!!!!!!!!!!!!!!!

    predicted_df = predicted_df.select(["vehicle_fuel", "vehicle_speed", "vehicle_noise", "vehicle_NOx", "prediction"])
    clustered_df = clustered_df.select(["vehicle_x", "vehicle_y", "congestion_cluster"])

    query_clustered = clustered_df \
        .writeStream \
        .format("csv") \
        .option("path", output_path_clustered) \
        .option("checkpointLocation", "checkpoint_dir") \
        .outputMode("append") \
        .start()

    query_predicted = predicted_df \
        .writeStream \
        .format("csv") \
        .option("path", output_path_predicted) \
        .option("checkpointLocation", "checkpoint_dir2") \
        .outputMode("append") \
        .start()

    # Vizuelizacija rezultata 
    # clustered_df = gpd.read_file(output_path_clustered)
    # fig, ax = plt.subplots(figsize=(10, 10))
    # gplt.pointplot(clustered_df, ax=ax, hue='cluster_label', legend=True, cmap='viridis')
    # plt.title('Klasteri zagađenja')
    # plt.xlabel('Longitude')
    # plt.ylabel('Latitude')
    # plt.savefig('klasteri_zagadjenja_mapa.png')
    # plt.show()

    # predicted_df = gpd.read_file(output_path_predicted)
    # fig, ax = plt.subplots(figsize=(10, 6))
    # predicted_df.plot(column='predicted_pollution', ax=ax, legend=True, cmap='plasma')
    # plt.title('Predviđeno zagađenje')
    # plt.xlabel('Vreme')
    # plt.ylabel('Zagađenje')
    # plt.xticks(rotation=45)
    # plt.grid(True)
    # plt.savefig('predvidjeno_zagadjenje_mapa.png')
    # plt.show()

    query_clustered.awaitTermination()
    query_predicted.awaitTermination()

    spark.stop()