from pyspark.sql import SparkSession
# import findspark
# findspark.init(
#     "/mnt/d/UbuntuWSL2/spark-3.1.2-bin-hadoop3.2/spark-3.1.2-bin-hadoop3.2")
TOPIC_NAME = "movies"
OUTPUT_TOPIC_NAME = "result"
BOOTSTRAP_SERVERS = 'localhost:9092'

spark = SparkSession \
    .builder \
    .appName("PySpark Structured Streaming with Kafka Demo") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("kafka").option(
    "kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("subscribe", TOPIC_NAME)\
    .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

checkpoint_path = "/mnt/d/UbuntuWSL2/checkpoint"
query = df.writeStream.format("kafka")\
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("topic", "result")\
    .option("checkpointLocation", checkpoint_path)\
    .start()
query.awaitTermination()

# query = df.writeStream.outputMode("append").format("console").start()
# query.awaitTermination()
