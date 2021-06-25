from pyspark.sql import SparkSession

MONGO_URI = "mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/movielens"


def importDataToMongo(data, spark):
    df = spark.read.option("header", True).csv("df.csv")
    df.printSchema()
    # df.write.format("mongo").mode("append").save()


def readFromMongo(collection, params, spark):
    df = spark.read.format("mongo").option(
        "uri", MONGO_URI+"."+collection).load()
    df.printSchema()


def writeToMongo(spark, data, collection):
    if data and spark and collection:
        data.write.format("mongo").option(MONGO_URI+".recommendation").save()
