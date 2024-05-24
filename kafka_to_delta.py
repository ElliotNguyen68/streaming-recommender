from pyspark.sql import SparkSession
from delta import  configure_spark_with_delta_pip
import pyspark
from const import TOPIC_STREAMING_CONSUMER
import pandas as pd
import os
import time
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,io.delta:delta-core_2.12:2.1.0  pyspark-shell'
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    # .config("spark.jars.repositories", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1")\
    # .config("spark.jars.repositories", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    # .config("org.apache.spark", "spark-sql-kafka-0-10_2.11:2.4.0 ")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", TOPIC_STREAMING_CONSUMER) \
  .load()
  
query = (
    df.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/tmp/abc")
    .start("abc4")
)
 
# query=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.format('console').start()
query.awaitTermination()