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

    return (
        three_shows.select("id", F.explode("_embedded.episodes.airstamp").alias("episode_timestamp"))
        .withColumn("episode_timestamp", F.to_timestamp("episode_timestamp"))
        .groupby("id")
        .agg(F.min("episode_timestamp").alias("min_timestamp"), F.max("episode_timestamp").alias("max_timestamp"))
        .withColumn("duration_show", F.datediff(F.col("max_timestamp"), F.col("min_timestamp")))
        .show(truncate=False)
    )


'''
Take the shows data frame and extract the air date and name of each episode in two array columns.
'''
def ex_six_seven():
    return (
        spark.read.json("../data/shows/shows-silicon-valley.json", multiLine=True)
        .select("_embedded.episodes.airdate", "_embedded.episodes.name")
        .show(truncate=False)
    )


'''
Given the following data frame, create a new data frame that contains a single map
from one to square:
exo6_8 = spark.createDataFrame([[1, 2], [2, 4], [3, 9]], ["one", "square"])
'''
def ex_six_eight():
    return (
        spark.createDataFrame([[1, 2], [2, 4], [3, 9]], ["one", "square"])
        .groupby()
        .agg(F.collect_list("one").alias("keys"), F.collect_list("square").alias("values"))
        .select(F.map_from_arrays("keys", "values").alias("number to number squared"))
        .show(truncate=False)
    )

