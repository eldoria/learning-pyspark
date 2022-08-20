from setup import *
import pyspark.sql.types as T

'''
ex 6.3

What is wrong with this schema?

schema = T.StructType([T.StringType(), T.LongType(), T.LongType()])
->
schema = T.StructType([T.StructField("name", T.StringType(), T.LongType(), T.LongType())])
----------------------------------
----------------------------------
ex 6.4

Why is it a bad idea to use the period or the square bracket in a column name, given
that you also use it to reach hierarchical entities within a data frame?

->
If i have in a dataframe a column who is a struct named "ex" and who have a field name "1" and an other field named
"ex.1"; I will not be able to use the second column because pyspark will read by default the first one.
----------------------------------
----------------------------------
ex 6.5

Although much less common, you can create a data frame from a dictionary. Since
dictionaries are so close to JSON documents, build the schema for ingesting the following dictionary. (Both JSON or PySpark schemas are valid here.)

dict_schema = ???
spark.createDataFrame([{"one": 1, "two": [1,2,3]}], schema=dict_schema)
'''
def ex_six_five():
    dict_schema = T.StructType(
        [
            T.StructField(
                "one", T.IntegerType()
            ),
            T.StructField(
                "two", T.ArrayType(T.IntegerType())
            )
        ]
    )
    spark.createDataFrame([{"one": 1, "two": [1, 2, 3]}], schema=dict_schema).show()

'''
ex 6.6

Using three_shows, compute the time between the first and last episodes for each
show. Which show had the longest tenure?
'''
def ex_six_six():
    three_shows = spark.read.json("../data/shows/shows-*.json", multiLine=True)
    three_shows.show()

    ## not finish yet
    times = (
        three_shows.select("id", F.explode("_embedded.episodes.airstamp").alias("episode_timestamp"))
        .groupby("id")
        .agg(F.min("episode_timestamp").alias("min_timestamp"), F.max("episode_timestamp").alias("max_timestamp"))
        .withColumn("duration_show", (F.col("max_timestamp").cast("long") - F.col("min_timestamp").cast("long")))
        .show()
    )

    #(F.min(F.col("_embedded.episodes.airstamp")).alias("min_timestamp"),
    #                   F.max(F.col("_embedded.episodes.airstamp")).alias("max_timestamp")).show()
