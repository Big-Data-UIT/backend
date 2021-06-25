from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

lines = spark.readStream.format("socket").option(
    "host", "localhost").option("port", "5555").load()

query = lines.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
# lines = spark.read.csv("ratings.csv", header=True)

# lines.printSchema()
