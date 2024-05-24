from pyspark.sql import SparkSession
from delta import  configure_spark_with_delta_pip
import pyspark
from const import TOPIC_STREAMING_CONSUMER
import pandas as pd
import os
import time

# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# spark = configure_spark_with_delta_pip(builder).getOrCreate()

# df =(
#     spark
#     .readStream
#     .format("rate")
#     .option("rowsPerSecond", 5)
#     .load('df_transaction')
# )

# # data = spark.range(0, 5)
# # (df.writeStream.format("delta")
# #   .outputMode("append")
# #   .option("truncate", False).option("checkpointLocation", "/tmp/delta/events/_checkpoints/").toTable("abc1")
# # )
# query=df.writeStream.format('delta').option("checkpointLocation", "/tmp/delta/events/_checkpoints/").trigger(processingTime="10 seconds").start("abc2")
# query.awaitTermination()
# format("delta")
#   .outputMode("append")
#   .option("checkpointLocation", "/tmp/delta/events/_checkpoints/")
#   .toTable("events")
from confluent_kafka import Consumer,Producer
import socket
from confluent_kafka.admin import AdminClient, NewTopic
admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092"
})
if TOPIC_STREAMING_CONSUMER not in admin_client.list_topics().topics.keys():
    topic =NewTopic(TOPIC_STREAMING_CONSUMER, 1, 1)
    admin_client.create_topics([topic,])



# conf = {'bootstrap.servers': 'host1:9092,host2:9092',
#         'group.id': 'foo',
#         'auto.offset.reset': 'smallest'}

# consumer = Consumer(conf)

# Initialize SparkSession
# spark = SparkSession.builder.appName("Stream to Delta").getOrCreate()

# Define your schema (adjust based on your specific schema)
# schema = StructType().add("column_name", "string")

# Read static data
data = pd.read_parquet('df_transaction')


# Convert DataFrame to list of Row objects
# data = df.collect()

# Emulate streaming data
def load_and_get_table_df(data):
    while len(data):
        slice = data.iloc[:5]
        data = data.iloc[5:]
        yield slice.to_json()   
        time.sleep(3)  # sleep for 1 second
        
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)
producer.produce(TOPIC_STREAMING_CONSUMER, key="key", value="value")
producer.flush()


for df in load_and_get_table_df(data):
    producer.produce(TOPIC_STREAMING_CONSUMER, key="key", value=df) 
# 
#     (df.writeStream
#         .outputMode("append")
#         .format("delta")
#         .option("checkpointLocation", "/path/to/checkpoint/directory")
#         .start("abc3")
#     )
#     print(1)
# # streaming_df = load_and_get_table_df()

# # # Write Stream to Delta Table
# # query = (
# #     streaming_df.writeStream
# #     .outputMode("append")
# #     .format("delta")
# #     .option("checkpointLocation", "/path/to/checkpoint/directory")
# #     .start("abc3")
# # )

# # # Wait for the stream to finish
# # query.awaitTermination()