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
print(collection_rdd.reduce(add)) # 24

