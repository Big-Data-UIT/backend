from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType
MONGO_URI = "mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/movielens"


def importDataToMongo(data, spark):
    df = spark.read.option("header", True).csv("df.csv")
    df.printSchema()
    # df.write.format("mongo").mode("append").save()


def readFromMongo(collection, spark):
    df = spark.read.format("mongo").option(
        "uri", MONGO_URI+"."+collection).load()
    return df


def writeDfToMongo(spark, data, collection):
    data.write.format("mongo").mode("append").option(
        "uri", MONGO_URI+".recommendation").save()


def writeToMongo(spark, data, collection):
    if data and spark and collection:
        schema = StructType([
            StructField('UserId', StringType(), True),
            StructField('Recommendation', ArrayType(StringType(), True), True),
        ])

        dtf = spark.createDataFrame(data, schema)
        dtf.show()
        dtf.write.format("mongo").option(
            "uri", MONGO_URI+".recommendation").save()
