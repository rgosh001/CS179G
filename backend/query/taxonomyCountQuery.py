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

#CREATE TABLE providers.taxonomy_count (
#	ZipCode varchar,	
#	Pop int,
#	MeanIncome int,
#	TaxonomyCode1 varchar,
#	ProviderNumber varchar,
#	HospitalName varchar,
#	X varchar,
#	Y varchar,
#	Count int,
#	PRIMARY KEY(ZipCode, TaxonomyCode1, ProviderNumber)
#);

if __name__ == "__main__":
        
    conf = SparkConf().setAppName("Query App").setMaster("spark://spark01.cs.ucr.edu:7077")
    sc = CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # pulling data from cassandra

    x = sc.cassandraTable("census", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("zipcode", "pop", "meanincome")\
        .map(lambda x: (x[0], x[1], x[2]))

    # BusinessPracticeLocationPostalCode -> ZipCode in providers.taxonomy_count
    y = sc.cassandraTable("providers", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("businesspracticelocationpostalcode", "taxonomycode1")\
	.map(lambda x: (x[0], x[1]))

    df_x = sqlContext.createDataFrame(x)
    df_y = sqlContext.createDataFrame(y)

    # joining x and y
    # (zipcode, pop, meanincome, zipcode, businesspracticelocationpostalcode, taxonomycode1) 
    # -> (zipcode, pop, meanincome, taxonomycode1) 
    # -> (zipcode, pop, meanincome, taxonomycode1, count) 

    def x_y_joinSeparator(): return lambda x: (x[0], x[1], x[2], x[4])

    cond = [df_x._1 == df_y._1]
    df_x_y = df_x.join(df_y, cond) \
			.map(x_y_joinSeparator()) \

    taxonomyCountRDD = df_x_y\
			.map(lambda x: (x, 1))\
			.reduceByKey(lambda x,y:x + y)\
			.map(lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[1]))

    customSchema = StructType([ \
    	StructField("ZipCode", StringType(), True), \
	StructField("Pop", StringType(), True), \
	StructField("MeanIncome", IntegerType(), True), \
	StructField("TaxonomyCode1", StringType(), True), \
	StructField("Count", IntegerType(), True)])

    taxonomyCountDF = sqlContext.createDataFrame(taxonomyCountRDD, customSchema)

    dfJSONRDD = taxonomyCountDF.toJSON().collect()

    mergedJSON = []
    for row in dfJSONRDD:
        mergedJSON.append(row)

    with open("/home/cs179g/json/taxonomyCount.json", "wb") as outfile:
        json.dump(mergedJSON, outfile)

    taxonomyCountRDD.map(lambda row: {'zipcode': row[0],
				   'pop' : row[1],
				   'meanincome' : row[2],
                                   'taxonomycode1' : row[3],
                                   'count' : row[4]}).saveToCassandra(keyspace='providers', table='taxonomy_count')

    sc.stop()
