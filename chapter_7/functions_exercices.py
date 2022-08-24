from pyspark.sql import DataFrame

from setup import *
from lesson import full_data

'''
ex 7.1

Taking the elements data frame, which PySpark code is equivalent to the following
SQL statement?

select count(*) from elements where Radioactive is not null;

a element.groupby("Radioactive").count().show()
b elements.where(F.col("Radioactive").isNotNull()).groupby().count().show()
c elements.groupby("Radioactive").where(F.col("Radioactive").isNotNull()).show()
d elements.where(F.col("Radioactive").isNotNull()).count()
e None of the queries above
->
b
----------------------------------
----------------------------------
ex 7.2:

If we look at the code that follows, we can simplify it even further and avoid creating
two tables outright. Can you write a summarized_data without having to use a table
other than full_data and no join? (Bonus: Try using pure PySpark, then pure Spark
SQL, and then a combo of both.)
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
'''


def ex_seven_two_only_pyspark():
    return (
        full_data
        .selectExpr("model", "capacity_bytes / pow(1024, 3) capacity_GB", "date", "failure")
        .groupby("model", "capacity_GB")
        .agg(
            F.sum(F.when(F.col("failure") == 1, 1).otherwise(0)).alias("failures"),
            # F.sum("failure").alias("failures"),
            F.count("*").alias("drive_days"),
        )
        .selectExpr("model", "capacity_GB", "failures / drive_days failure_rate")
        .show()
    )


def ex_seven_two_only_sql():
    full_data.createTempView("drive_stats")
    return spark.sql(
        """
        with drive_days as (
            select model, count(*) as drive_days
            from drive_stats
            group by model
        ),
        failures as (
            select model, count(*) as failures
            from drive_stats
            where failure == 1
            group by model
        )
        
        select d.model, d.drive_days / f.failures as failure_ratio
        from drive_days d inner join failures f on d.model = f.model
        """
    ).show(20)


'''
The analysis in the chapter is flawed in that the age of a drive is not taken into consideration. 
Instead of ordering the model by failure rate, order by average age at failure 
(assume that every drive fails on the maximum date reported if they are still alive).
(Hint: Remember that you need to count the age of each drive first.)
'''
def ex_seven_three():
    full_data.createTempView("drive_stats")
    return spark.sql(
        """
        with failure_date as (
            select model, min(date) as dateFailure
            from drive_stats
            where failure == 1
            group by model
        ),
        last_date as (
            select model, max(date) as dateFailure
            from drive_stats
            group by model
        )
        
        select model, dateFailure from
        (
            select f.model, case when f.dateFailure is not null then f.dateFailure else l.dateFailure end as dateFailure
            from failure_date f inner join last_date l on f.model = l.model
        )
        order by dateFailure
        """
    ).show(20)

