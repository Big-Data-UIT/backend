from pyspark.sql import SparkSession
from pyspark import SparkContext
from utils import getOrCreateSparkSession
from import_mongo import writeDfToMongo, readFromMongo, writeToMongo
from CFAlgo import CF
TOPIC_NAME = "movies"
OUTPUT_TOPIC_NAME = "result"
BOOTSTRAP_SERVERS = 'localhost:9092'


spark = getOrCreateSparkSession("full")
spark.sparkContext.setLogLevel("ERROR")


#  read new data
rowdf = spark.readStream.format("kafka").option(
    "kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("subscribe", TOPIC_NAME)\
    .load()

# write to db first
writeDfToMongo(spark, rowdf, "ratings_copy")


def processRecommendation():
    #  read from db and process
    df = readFromMongo("ratings_copy", spark)
    df = df.drop("_id")
    df = df.drop("timestamp")
    df = df[['userId', 'movieId', 'rating']]
    df.withColumn("movieId", df.movieId.cast('string'))
    cf = CF(spark, df)
    result = cf.processRecommendations()
    # print(result)
    writeToMongo(spark, result, "recommendation")


processRecommendation()

checkpoint_path = "/mnt/d/UbuntuWSL2/checkpoint"
query = rowdf.writeStream.format("kafka")\
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("topic", "result")\
    .option("checkpointLocation", checkpoint_path)\
    .start()

query.awaitTermination()
# query = df.writeStream.outputMode("append").format("console").start()
# query.awaitTermination()
# lines.printSchema()
