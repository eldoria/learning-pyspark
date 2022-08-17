from setup import *

'''
Reread the data in a logs_raw data frame (the data file is ./data/broadcast_logsBroadcastLogs_2018_Q3_M8.CSV),
this time without passing any optional parameters.
Print the first five rows of data, as well as the schema. What are the differences in terms of data and schema between 
logs and logs_raw?
'''
def ex_four_three(path):
    df = spark.read.csv(path)
    df.printSchema()
    df.show(5, False)


'''
Create a new data frame, logs_clean, that contains only the columns that do not end with ID.
'''
def ex_four_four(path):
    logs = spark.read.csv(path, sep="|", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd")
    logs_clean = logs.select([col for col in logs.columns if col[-2:] != "ID"]).show(5, False)

    return logs_clean
