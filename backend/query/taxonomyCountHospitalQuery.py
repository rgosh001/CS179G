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
#	TaxonomyCode1 varchar,
#	ProviderNumber varchar,
#	HospitalName varchar,
#	X varchar,
#	Y varchar,
#	Count int,
#	MeanIncome int,
#	PRIMARY KEY(ZipCode, TaxonomyCode1, ProviderNumber)
#);

if __name__ == "__main__":
        
    conf = SparkConf().setAppName("Query App").setMaster("spark://spark01.cs.ucr.edu:7077")
    sc = CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # pulling data from cassandra

    x = sc.cassandraTable("census", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("zipcode", "pop", "meanincome")\
        .map(lambda x: (x[0], x[1], x[2]))

    y = sc.cassandraTable("hospitals", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("zipcode", "providernumber", "hospitalname", "x", "y")\
        .map(lambda x: (x[0], x[1], x[2], x[3], x[4]))

    # BusinessPracticeLocationPostalCode -> ZipCode in providers.taxonomy_count
    z = sc.cassandraTable("providers", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("businesspracticelocationpostalcode", "taxonomycode1")\
	.map(lambda x: (x[0], x[1]))

    # num of hospitals RDD (zipcode, numberofhospitals)
    a = sc.cassandraTable("hospitals", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("zipcode")\
	.map(lambda x: (x, 1))\
	.reduceByKey(add)\
	.map(lambda x: (x[0][0], x[1]))


    df_x = sqlContext.createDataFrame(x)
    df_y = sqlContext.createDataFrame(y)
    df_z = sqlContext.createDataFrame(z)
    df_a = sqlContext.createDataFrame(a)

    # joining x and y
    # (zipcode, pop, meanincome, zipcode, providernumber, hospitalname, x, y) -> (zipcode, pop, meanincome, providernumber, hospitalname, x, y)
    def x_y_joinSeparator(): return lambda x: (x[0], x[1], x[2], x[4], x[5], x[6], x[7])

    cond = [df_x._1 == df_y._1]
    x_y = df_x.join(df_y, cond) \
			.map(x_y_joinSeparator())

    df_x_y = sqlContext.createDataFrame(x_y)

    # joining y and z
    # (zipcode, pop, meanincome, providernumber, hospitalname, x, y, businesspra..alcode, taxonomycode1) 
    # -> (zipcode, pop, meanincome, providernumber, hospitalname, x, y, taxonomycode1)
    # -> (zipcode, pop, meanincome, providernumber, hospitalname, x, y, taxonomycode1, count)
    # assume zipcode == postalcode
    # later trunc last 4 char of postalcode
    def x_y_z_joinSeparator(): return lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[8])

    cond = [df_x_y._1 == df_z._1]
    x_y_z = df_x_y.join(df_z, cond) \
			.map(x_y_z_joinSeparator()) \
			.map(lambda x: (x, 1))\
			.reduceByKey(lambda x,y:x + y)\
			.map(lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[0][4], x[0][5], x[0][6], x[0][7], x[1]))

    df_x_y_z = sqlContext.createDataFrame(x_y_z)

    # joining x_y_z and a
    # (zipcode, pop, meanincome, providernumber, hospitalname, x, y, taxonomycode1, count, zipcode, numberofhospitals)
    # -> (zipcode, pop, meanincome, providernumber, hospitalname, x, y, taxonomycode1, count, numberofhospitals)
    def x_y_z_a_joinSeparator(): return lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[10])

    cond = [df_x_y_z._1 == df_a._1]
    taxonomyCountRDD = df_x_y_z.join(df_a, cond) \
			.map(x_y_z_a_joinSeparator()) \

    customSchema = StructType([ \
    	StructField("ZipCode", StringType(), True), \
	StructField("Pop", StringType(), True), \
	StructField("MeanIncome", IntegerType(), True), \
	StructField("ProviderNumber", StringType(), True), \
    	StructField("HospitalName", StringType(), True), \
	StructField("X", StringType(), True), \
	StructField("Y", StringType(), True), \
	StructField("TaxonomyCode1", StringType(), True), \
	StructField("Count", IntegerType(), True), \
	StructField("NumberOfHospitals", IntegerType(), True)])

    taxonomyCountDF = sqlContext.createDataFrame(taxonomyCountRDD, customSchema)

    dfJSONRDD = taxonomyCountDF.toJSON().collect()

    mergedJSON = []
    for row in dfJSONRDD:
        mergedJSON.append(row)

    with open("/home/cs179g/json/taxonomyCountHospital.json", "wb") as outfile:
        json.dump(mergedJSON, outfile)

    taxonomyCountRDD.map(lambda row: {'zipcode': row[0],
				   'pop' : row[1],
				   'meanincome' : row[2],
				   'providernumber' : row[3],
				   'hospitalname' : row[4],
				   'x' : row[5],
				   'y' : row[6],
                                   'taxonomycode1' : row[7],
                                   'count' : row[8],
				   'numberofhospitals' : row[9]}).saveToCassandra(keyspace='providers', table='taxonomy_count_hospital')


    sc.stop()
