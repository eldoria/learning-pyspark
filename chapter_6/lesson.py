from setup import *
import pyspark.sql.types as T

'''
shows = spark.read.json("../data/shows/shows-silicon-valley.json")

columns = ["name", "language", "type"]
shows_map = shows.select(
 *[F.lit(column) for column in columns],
 F.array(*columns).alias("values"),
)
shows_map.show(1, False)
shows_map = shows_map.select(F.array(*columns).alias("keys"), "values")
shows_map.show(1, False)

shows_map = shows_map.select(
 F.map_from_arrays("keys", "values").alias("mapped")
)

shows_map.show(1, False)
'''
from py4j.protocol import Py4JJavaError

episode_links_schema = T.StructType(
    [
        T.StructField(
            "self", T.StructType([T.StructField("href", T.StringType())])
        )
    ]
)
episode_image_schema = T.StructType(
    [
        T.StructField("medium", T.StringType()),
        T.StructField("original", T.StringType()),
    ]
)

episode_schema = T.StructType(
    [
        T.StructField("_links", episode_links_schema),
        T.StructField("airdate", T.DateType()),
        T.StructField("airstamp", T.TimestampType()),
        T.StructField("airtime", T.StringType()),
        T.StructField("id", T.StringType()),
        T.StructField("image", episode_image_schema),
        T.StructField("name", T.StringType()),
        T.StructField("number", T.LongType()),
        T.StructField("runtime", T.LongType()),
        T.StructField("season", T.LongType()),
        T.StructField("summary", T.LongType()),  # error here
        T.StructField("url", T.LongType()),  # error here
    ]
)

embedded_schema = T.StructType(
    [
        T.StructField(
            "_embedded",
            T.StructType(
                [
                    T.StructField(
                        "episodes", T.ArrayType(episode_schema)
                    )
                ]
            ),
        )
    ]
)

shows_with_schema_wrong = spark.read.json(
    "../data/shows/shows-silicon-valley.json",
    schema=embedded_schema,
    mode="FAILFAST",
)
try:
    shows_with_schema_wrong.show()
except Py4JJavaError:
    pass

'''
shows_with_schema = spark.read.json(
    "../data/shows/shows-silicon-valley.json",
    schema=embedded_schema,
    mode="FAILFAST"
)

for column in ["airdate", "airstamp"]:
    shows_with_schema.select(f"_embedded.episodes.{column}").select(
        F.explode(column).alias(column)
    ).show(5)
'''
