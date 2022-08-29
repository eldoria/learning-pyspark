from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext
