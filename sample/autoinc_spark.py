import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext

# Spark Application
sc = SparkContext("local", "post-sales-auto")
raw_rdd = sc.textFile("data.csv")

def extract_vin_key_value(row):
    auto = row.strip().split(",")
    return (auto[2], (auto[1], auto[3], auto[5]))

# Extract VIN for key
# Extract vehicle make and year along with incident type for value
vin_kv = raw_rdd.map(lambda row: extract_vin_key_value(row))

def populate_make(row):

    group_master = []
    for i in row:
        group_master.append(list(i))
        
    for i in range(0, len(group_master)):
        make = group_master[i-1][1]
        year = group_master[i-1][2]
        
        if group_master[i][0] != 'I':
            if group_master[i][1] == '' and group_master[i][2] == '':
                group_master[i][1] = make
                group_master[i][2] = year
        
    return group_master

# Populate vehicle make and year for records missing these values
enhance_make = vin_kv.groupByKey().flatMap(lambda row: populate_make(row[1]))

def extract_make_key_value(row):

    if row[0] == 'A':
        return (row[1], row[2]), 1
    else:
        return (row[1], row[2]), 0

# Filter records that are not accident incidents
make_kv = enhance_make.map(lambda x: extract_make_key_value(x))

# Count accident incidents by make and year
accidents = make_kv.reduceByKey(lambda x, y: x + y)

# Write to file
file = open('accidents.txt', 'x')
for accident in accidents.collect():
    if accident[1] >= 1:
        file.write('{}-{},{}\n'.format(accident[0][0], accident[0][1], accident[1]))
file.close()