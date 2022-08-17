import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Counting word occurences from a book."
).getOrCreate()

'''
By modifying the word_count_submit.py program, return the number of distinct words in Jane Austen’s Pride and Prejudice.
(Hint: results contains one record for each unique word.)
(Challenge) Wrap your program in a function that takes a file name as a parameter.
It should return the number of distinct words.
'''
def ex_three_three(path):
    return (
        spark.read.text(path)
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
        .where(F.col("word") != "")
        .groupby(F.col("word"))
        .count()
        .count()
    )

'''
Taking word_count_submit.py, modify the script to return a sample of five words that appear only once in 
Jane Austen’s Pride and Prejudice.
'''
def ex_three_four(path):
    return (
        spark.read.text(path)
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
        .where(F.col("word") != "")
        .groupby(F.col("word"))
        .count()
        .where(F.col("count") == 1)
        .show(5)
    )

'''
Using the substring function (refer to PySpark’s API or the pyspark shell if needed),
return the top five most popular first letters (keep only the first letter of each word).
'''
def ex_three_five_one(path):
    return (
        spark.read.text(path)
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
        .where(F.col("word") != "")
        .select(F.substring(F.col("word"), 1, 1).alias("letter"))
        .groupby(F.col("letter"))
        .count()
        .sort(F.col("count").desc())
        .show(5)
    )


'''
Compute the number of words starting with a consonant or a vowel. (Hint: The
isin() function might be useful.)
'''
def ex_three_five_two(path):
    return (
        spark.read.text(path)
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
        .where(F.col("word") != "")
        .select(F.substring(F.col("word"), 1, 1).alias("letter"))
        .groupby(F.col("letter"))
        .count()
        .select(F.col("letter"), F.col("count"), F.col("letter").isin(["a", "e", "i", "o", "u", "y"]).alias("isVowel"))
        .groupby(F.col("isVowel"))
        .sum("count")
        .show()
    )
