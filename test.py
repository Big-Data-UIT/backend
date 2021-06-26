from pyspark.sql import SparkSession
from pyspark import SparkContext
from utils import getOrCreateSparkSession
TOPIC_NAME = "movies"
OUTPUT_TOPIC_NAME = "result"
BOOTSTRAP_SERVERS = 'localhost:9092'


spark = getOrCreateSparkSession("kafka")
spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("kafka").option(
    "kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("subscribe", TOPIC_NAME)\
    .load()

checkpoint_path = "/mnt/d/UbuntuWSL2/checkpoint"
query = df.writeStream.format("kafka")\
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("topic", "result")\
    .option("checkpointLocation", checkpoint_path)\
    .start()
query.awaitTermination()

# query = df.writeStream.outputMode("append").format("console").start()
# query.awaitTermination()
# lines.printSchema()
