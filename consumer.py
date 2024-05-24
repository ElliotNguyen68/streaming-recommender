from pyspark.sql import SparkSession, functions as F
from delta import *
import pyspark

import os

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


df = spark.read.format('delta').load("abc2")
# df.groupBy('contact_key').agg(
#     F.count_distinct('product_key')
# ).start().awaitTermination()
df.show()