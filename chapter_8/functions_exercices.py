from setup import sc


'''
The PySpark RDD API provides a count() method that returns the number of elements in the RDD as an integer.
Reproduce the behavior of this method using map(), filter(), and/or reduce().
'''
def ex_eight_one():
    ex_rdd = sc.parallelize([1, "two", 3.0, ("four", 4), {"five": 5}])

    print(ex_rdd.reduce(lambda x: x+1))
