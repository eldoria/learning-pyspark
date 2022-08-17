from setup import *


'''
ex 5.4:

Write PySpark code that will return the result of the following code block without
using a left anti-join:

left.join(right, how="left_anti", on="my_column").select("my_column").distinct()
->
left.join(right, how="left", on="my_column").where(right["my_column"].isnull()).select("my_column").distinct()
'''

'''
Using the data from the data/broadcast_logs/Call_Signs.csv (careful: the delimiter here is the comma, not the pipe!), 
add the Undertaking_Name to our final table to display a human-readable description of the channel.
'''
def ex_five_five(left):
    right = spark.read.csv("../data/broadcast_logs/Call_Signs.csv", sep=',', header=True, inferSchema=True)
    return (
        left.join(right, how="left", on="LogIdentifierID")
            .show(5)
    )

'''
The government of Canada is asking for your analysis, but they’d like the PRC to be
weighted differently. They’d like each PRC second to be considered 0.75 commercial
seconds. Modify the program to account for this change.
'''
