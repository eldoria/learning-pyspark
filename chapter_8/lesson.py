from setup import spark

collection = [1, "two", 3.0, ("four", 4), {"five": 5}]
sc = spark.sparkContext
collection_rdd = sc.parallelize(collection)


def safer_add_one(value):
    try:
        return value + 1
    except TypeError:
        return value


collection_rdd = collection_rdd.map(safer_add_one)
print(collection_rdd.collect())

collection_rdd = collection_rdd.filter(
    lambda elem: isinstance(elem, (float, int))
)
print(collection_rdd.collect())

from operator import add

collection_rdd = sc.parallelize([4, 7, 9, 1, 3])
print(collection_rdd.reduce(add))  # 24

################
################
import pyspark.sql.functions as F
import pyspark.sql.types as T

fractions = [[x, y] for x in range(100) for y in range(1, 100)]
frac_df = spark.createDataFrame(fractions, ["numerator", "denominator"])
frac_df = frac_df.select(
    F.array(F.col("numerator"), F.col("denominator")).alias(
        "fraction"
    ),
)
frac_df.show(5, False)

################
################

from fractions import Fraction
from typing import Tuple, Optional

Frac = Tuple[int, int]


def py_reduce_fraction(frac: Frac) -> Optional[Frac]:
    """Reduce a fraction represented as a 2-tuple of integers."""
    num, denom = frac
    if denom:
        answer = Fraction(num, denom)
        return answer.numerator, answer.denominator
    return None


assert py_reduce_fraction((3, 6)) == (1, 2)
assert py_reduce_fraction((1, 0)) is None


################
################
def py_fraction_to_float(frac: Frac) -> Optional[float]:
    """Transforms a fraction represented as a 2-tuple of integers into a
    float."""
    num, denom = frac
    if denom:
        return num / denom
    return None


assert py_fraction_to_float((2, 8)) == 0.25
assert py_fraction_to_float((10, 0)) is None

################
################

SparkFrac = T.ArrayType(T.LongType())
reduce_fraction = F.udf(py_reduce_fraction, SparkFrac)
frac_df = frac_df.withColumn(
    "reduced_fraction", reduce_fraction(F.col("fraction"))
)
frac_df.show(5, False)


################
################

@F.udf(T.DoubleType())
def fraction_to_float(frac: Frac) -> Optional[float]:
    """Transforms a fraction represented as a 2-tuple of integers into a float."""
    num, denom = frac
    if denom:
        return num / denom
    return None


frac_df = frac_df.withColumn(
    "fraction_float", fraction_to_float(F.col("reduced_fraction"))
)
frac_df.select("reduced_fraction", "fraction_float").distinct().show(
    5, False
)
