from setup import *

from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F
import pyspark.sql.types as T

elements = spark.read.csv(
    "../data/elements/Periodic_Table_Of_Elements.csv",
    header=True,
    inferSchema=True,
)

elements.createTempView("elements")

spark.sql(
    "select period, count(*) from elements where phase='liq' group by period"
).show(5)
