from pyspark.sql import SparkSession
from operator import add


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
'''