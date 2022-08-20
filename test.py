import pyspark.sql.functions as F
from setup import *


shows_with_schema = spark.read.json(
    "./data/shows/shows-silicon-valley.json",
    mode="FAILFAST"
)

shows_with_schema.show(5)

for column in ["airdate", "airstamp"]:
    shows_with_schema.select(f"_embedded.episodes.{column}").select(
        F.explode(column).alias(column)
    ).show(5)

