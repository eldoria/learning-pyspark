from pyspark.sql import SparkSession
from operator import add
import pyspark.sql.types as T
import pyspark.sql.functions as F
from typing import Tuple, Optional
from fractions import Fraction

Frac = Tuple[int, int]


'''
The PySpark RDD API provides a count() method that returns the number of elements in the RDD as an integer.
Reproduce the behavior of this method using map(), filter(), and/or reduce().
'''
def ex_eight_one():
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    collection_rdd = sc.parallelize([1, "two", 3.0, ("four", 4), {"five": 5}])

    collection_rdd = collection_rdd.map(lambda _: 1).reduce(add)
    print(collection_rdd)


'''
ex 8.2:

What is the return value of the following code block?

a_rdd = sc.parallelize([0, 1, None, [], 0.0])
a_rdd.filter(lambda x: x).collect()

a [1]
b [0, 1]
c [0, 1, 0.0]
d []
e [1, []]

->
a because in python empty sequences and 0/0.0 are considered false

################
################

ex 8.3:

Using the following definitions, create a temp_to_temp(value, from, to) that takes a
numerical value in from degrees and converts it to degrees.
- C = (F - 32) * 5 / 9 (Celcius)
- K = C + 273.15 (Kelvin)
- R = F + 459.67 (Rankine)
'''
def ex_eight_three():
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    df = spark.createDataFrame([[i] for i in range(100)], ["Fahrenheit"])
    df.show(5)
    df = df.withColumn(
        "Celsius", temp_to_temp(F.col("Fahrenheit"), F.lit("f"), F.lit("c"))
    )
    df.show(5)


@F.udf(T.DoubleType())
def temp_to_temp(value: int, _from: str, _to: str) -> float:
    if _from == _to:
        return value

    def f_to_c(f):
        return (f-32) * 5 / 9

    def c_to_k(c):
        return c + 273.15

    def f_to_r(f):
        return f + 459.67

    if _from == "f" and _to == "c":
        return f_to_c(value)
    if _from == "c" and _to == "k":
        return c_to_k(value)
    if _from == "f" and _to == "k":
        return c_to_k(f_to_c(value))
    if _from == "f" and _to == "r":
        return f_to_r(value)


'''
ex 8.4:

Correct the following UDF, so it doesn’t generate an error.
@F.udf(T.IntegerType())
def naive_udf(t: str) -> str:
    return answer * 3.14159
    
->

@F.udf(T.FloatType())
def naive_udf(value: float) -> float:
    return value * 3.14159
    
    
ex 8.5:

Create a UDF that adds two fractions together, 
and test it by adding the reduced_fraction to itself in the test_frac data frame.
'''
def ex_eight_five():
    spark = SparkSession.builder.getOrCreate()

    fractions = [[x, y] for x in range(1, 3+1) for y in range(4, 7+1)]
    frac_df = spark.createDataFrame(fractions, ["numerator", "denominator"])
    frac_df = frac_df.select(
        F.array(F.col("numerator"), F.col("denominator")).alias(
            "fraction"
        ),
    )
    frac_df = frac_df.withColumn("fraction 1/10", F.array(F.lit(1), F.lit(10)))
    frac_df.show(20)
    frac_df = frac_df.withColumn(
        "fraction + 1/10", fraction_addition(F.col("fraction"), F.col("fraction 1/10"))
    )


@F.udf(T.FractionalType)
def fraction_addition(frac1: Frac, frac2: Frac) -> Optional[Frac]:
    """Transforms a fraction represented as a 2-tuple of integers into a float."""
    num1, denom1 = frac1
    num2, denom2 = frac2

    if denom1 and denom2:
        answer = Fraction(num1+num2, denom1+denom2)
        return answer.numerator, answer.denominator
    return None
