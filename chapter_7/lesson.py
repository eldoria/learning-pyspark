from setup import *
from functools import reduce
import pyspark.sql.functions as F

DATA_DIRECTORY = "../data/backblaze/"
DATA_FILES = [
    "drive_stats_2019_Q1"#,
    #"data_Q2_2019",
    #"data_Q3_2019",
    #"data_Q4_2019",
]
data = [
    spark.read.csv(DATA_DIRECTORY + file, header=True, inferSchema=True)
    for file in DATA_FILES
]
common_columns = list(
    reduce(lambda x, y: x.intersection(y), [set(df.columns) for df in data])
)
assert set(["model", "capacity_bytes", "date", "failure"]).issubset(
    set(common_columns)
)
full_data = reduce(
    lambda x, y: x.select(common_columns).union(y.select(common_columns)), data
)#.createTempView("drive_stats")
'''
full_data = full_data.selectExpr(
    "model", "capacity_bytes / pow(1024, 3) capacity_GB", "date", "failure"
)

drive_days = full_data.groupby("model", "capacity_GB").agg(
    F.count("*").alias("drive_days")
)

failures = (
    full_data.where("failure = 1")
    .groupby("model", "capacity_GB")
    .agg(F.count("*").alias("failures"))
)
summarized_data = (
    drive_days.join(failures, on=["model", "capacity_GB"], how="left")
    .fillna(0.0, ["failures"])
    .selectExpr("model", "capacity_GB", "failures / drive_days failure_rate")
    .cache()
)


def most_reliable_drive_for_capacity(data, capacity_GB=2048, precision=0.25,
                                     top_n=3):
    """Returns the top 3 drives for a given approximate capacity.
    Given a capacity in GB and a precision as a decimal number, we keep the N
    drives where:
    - the capacity is between (capacity * 1/(1+precision)), capacity *
   (1+precision)
    - the failure rate is the lowest
    """
    capacity_min = capacity_GB / (1 + precision)
    capacity_max = capacity_GB * (1 + precision)
    answer = (
        data.where(f"capacity_GB between {capacity_min} and {capacity_max}")
        .orderBy("failure_rate", "capacity_GB", ascending=[True, False])
        .limit(top_n)
    )
    return answer


most_reliable_drive_for_capacity(summarized_data, capacity_GB=11176.0).show()
'''
