from pyspark.sql import SparkSession

def importDataToMongo():
    spark = SparkSession.builder.appName("SimpleApp")\
                    .config("spark.mongodb.input.uri","mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/movielens.movies")\
                    .config("spark.mongodb.output.uri","mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/movielens.movies")\
                    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0")\
                    .getOrCreate()
    df = spark.read.option("header",True).csv("df.csv")
    df.printSchema()
    df.write.format("mongo").mode("append").save()
            # .option("spark.mongodb.input.uri","mongodb+srv://carie_admin:carie.admin@cluster0.fteep.mongodb.net/movielens.movies")\
            