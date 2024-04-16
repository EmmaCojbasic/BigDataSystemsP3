import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator, ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import RandomForestRegressor


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

if __name__ == '__main__':
    if len(sys.argv) < 1:
        print("Usage: main.py <input folder> ")
        exit(-1)

    REGRESSION_MODEL_LOCATION = os.getenv('REGRESSION_MODEL_LOCATION')
    CLUSTER_MODEL_LOCATION = os.getenv('CLUSTER_MODEL_LOCATION')

    spark = SparkSession.builder \
        .appName("ModelCreation") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    emission_file = 'hdfs://namenode:9000/dir/my_emission_file.csv'
    fcd_file = 'hdfs://namenode:9000/dir/sumoTrace.csv'

    dfEmission = spark.read.option("header", "true").option("delimiter", ";").csv(emission_file, inferSchema=True)
    dfEmission=dfEmission.limit(1000)
    dfEmission.show(10)

    dfFcd = spark.read.option("header", "true").option("delimiter", ";").csv(fcd_file, inferSchema=True)
    dfFcd=dfFcd.limit(1000)
    dfFcd.show(10)

    fcd_features = ["vehicle_x", "vehicle_y"]
    kmeans_assembler = VectorAssembler(inputCols=fcd_features, outputCol="fcd_features")
    #dfFcd = kmeans_assembler.transform(dfFcd)
    #finalDfFcd = dfFcd.select("fcd_features")

    emission_features = ["vehicle_fuel", "vehicle_speed", "vehicle_noise"] #, "vehicle_type"]
    regression_assembler = VectorAssembler(inputCols=emission_features, outputCol="emission_features")
    #dfEmission = regression_assembler.transform(dfEmission)
    #finalDfEmission = dfEmission.select("emission_features", "vehicle_NOx")

    (trainFcd, testFcd) = dfFcd.randomSplit([0.7, 0.3], seed=42)
    (trainEmission, testEmission) = dfEmission.randomSplit([0.7, 0.3], seed=42)
   
    kmeans = KMeans(k=5, seed=1, featuresCol="fcd_features", predictionCol="congestion_cluster")
    kmeans_pipeline = Pipeline(stages=[kmeans_assembler, kmeans])
    kmeans_model = kmeans_pipeline.fit(trainFcd)
    predictions2 = kmeans_model.transform(testFcd)

    rf = RandomForestRegressor(featuresCol='emission_features', labelCol='vehicle_NOx', maxDepth=10)
    rf_pipeline = Pipeline(stages=[regression_assembler, rf])
    rf_model = rf_pipeline.fit(trainEmission)
    predictions = rf_model.transform(testEmission)

    congestion_model_location = os.getenv('CONGESTION_MODEL_LOCATION')
    pollution_model_location = os.getenv('POLLUTION_MODEL_LOCATION')

    kmeans_model.write().overwrite().save('hdfs://namenode:9000/dir2/cmodel')
    rf_model.write().overwrite().save('hdfs://namenode:9000/dir2/rmodel')

    # Evaluacija regresora

    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="vehicle_NOx", metricName="mse")
    accuracy = evaluator.evaluate(predictions)
    print("Mean Squared error modela: ", accuracy)

    f1_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="vehicle_NOx", metricName="rmse")
    f1_score = f1_evaluator.evaluate(predictions)
    print("Root Mean Squared error modela: ", f1_score)

    recall_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="vehicle_NOx", metricName="mae")
    recall = recall_evaluator.evaluate(predictions)
    print("Mean Absolute error modela: ", recall)

    # Evaluacija algoritma za klasterizaciju

    evaluator = ClusteringEvaluator(predictionCol="congestion_cluster", featuresCol="fcd_features", metricName="silhouette")
    silhouette = evaluator.evaluate(predictions2)
    print("Silhouette Score:", silhouette)


    spark.stop()
