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
    
    x = sc.cassandraTable("census", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("zipcode", "pop", "meanincome")\
        .map(lambda x: (x[0], x[1], x[2])) \

    y = sc.cassandraTable("hospitals", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("zipcode", "numberofdoctors")\
	.map(lambda x: (x[0], x[1]))\
	.reduceByKey(lambda x,y: x + y) 

    df_x = sqlContext.createDataFrame(x)
    df_y = sqlContext.createDataFrame(y)

    # (zipcode, pop, meanincome, zipcode, numberofdoctors) -> (zipcode, pop, meanincome, numberofdoctors, pop/numberofdoctors)
    def joinSeparator(): return lambda x: (x[0], x[1], x[2], x[4], float(x[1])/x[4])

    cond = [df_x._1 == df_y._1]
    popDoctorRatioQueryRDD = df_x.join(df_y, cond) \
			.map(joinSeparator()) \

    #for row in popDoctorRatioRDD:
	#print (row)

    dfList = popDoctorRatioQueryRDD.collect(); # if you do left outer join, the results that are null for the census show up because hospitals have them

    customSchema = StructType([ \
    	StructField("ZipCode", StringType(), True), \
	StructField("Pop", IntegerType(), True), \
	StructField("MeanIncome", IntegerType(), True), \
        StructField("NumberOfDoctors", IntegerType(), True), \
	StructField("Ratio", FloatType(), True)])

    popDoctorRatioQueryDF = sqlContext.createDataFrame(popDoctorRatioQueryRDD, customSchema)

    dfJSONRDD = popDoctorRatioQueryDF.toJSON().collect()

    mergedJSON = []
    for row in dfJSONRDD:
        mergedJSON.append(row)

    with open("/home/cs179g/json/popDoctorRatio.json", "wb") as outfile:
        json.dump(mergedJSON, outfile)

    popDoctorRatioQueryRDD.map(lambda row: {'zipcode': row[0],
                                   'pop': row[1],
				   'meanincome': row[2],
                                   'numberofdoctors': row[3],
                                   'ratio': row[4]}).saveToCassandra(keyspace='hospitals', table='pop_doctor_ratio')

    sc.stop()
