FROM bitnami/spark:latest

# COPY . /app
# RUN apt-get update && \
#     apt-get install -y gcc g++ gfortran libblas-dev liblapack-dev
#RUN apk add --no-cache py3-numpy

COPY . /app
# COPY requirements.txt /app/
RUN cd /app && pip install -r requirements.txt

ENV KAFKA_URL kafka:9092
ENV WINDOW_DURATION "1 minute"
ENV CLUSTER_MODEL_LOCATION="hdfs://namenode:9000/dir2/cmodel"
ENV REGRESSION_MODEL_LOCATION="hdfs://namenode:9000/dir2/rmodel"

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/streaming.py
ENV SPARK_MASTER_NAME spark
ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_ARGS "hdfs://namenode:9000/dir2/cmodel"
ENV SPARK_SUBMIT_ARGS "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

COPY submit.sh /
CMD ["/bin/bash", "/submit.sh"]