from __future__ import print_function

import sys
import time
import pyspark_cassandra
import json as json
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark_cassandra import CassandraSparkContext

from operator import add

start_time = time.time()

if __name__ == "__main__":
        
    conf = SparkConf().setAppName("Query App").setMaster("spark://spark01.cs.ucr.edu:7077")
    sc = CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # pulling data from cassandra
    
    hospitalNameList = sc.cassandraTable("hospitals", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("hospitalname", "numberofdoctors")\
	.map(lambda x: (x[0], x[1]))\
	.reduceByKey(lambda x,y: x + y) 

    #for row in popDoctorRatioRDD:
	#print (row)

    dfList = hospitalNameList.collect(); # if you do left outer join, the results that are null for the census show up because hospitals have them

    customSchema = StructType([ \
	StructField("HospitalName", StringType(), True), \
        StructField("NumberOfDoctors", IntegerType(), True) ])

    hospitalNameDF = sqlContext.createDataFrame(dfList, customSchema)

    dfJSONRDD = hospitalNameDF.toJSON().collect()

    mergedJSON = []
    for row in dfJSONRDD:
        mergedJSON.append(row)

    with open("/home/cs179g/json/numberOfDoctors.json", "wb") as outfile:
        json.dump(mergedJSON, outfile)

    hospitalNameList.map(lambda row: {'hospitalname': row[0],
                                   'numberofdoctors': row[1]}).saveToCassandra(keyspace='hospitals', table='hospital_numberofdoctors')

    sc.stop()
