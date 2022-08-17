from setup import *


logs = (
    spark.read.csv("../data/broadcast_logs/BroadcastLogs_2018_Q3_M8.CSV", sep="|", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd")
    .drop("BroadcastLogID", "SequenceNO")
    .withColumn(
        "Duration_seconds",
        (
            F.col("Duration").substr(1, 2).cast("int") * 3600 +
            F.col("Duration").substr(4, 2).cast("int") * 60 +
            F.col("Duration").substr(7, 2).cast("int")
        )
    )
)

for i in logs.columns:
    logs.describe(i).show()
