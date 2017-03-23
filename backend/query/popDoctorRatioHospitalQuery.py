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
    
    # num of hospitals RDD (zipcode, numofhospitals)
    x = sc.cassandraTable("hospitals", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("zipcode")\
	.map(lambda x: (x, 1))\
	.reduceByKey(add)\
	.map(lambda x: (x[0][0], x[1]))

    y = sc.cassandraTable("hospitals", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("zipcode", "providernumber", "hospitalname", "x", "y", "numberofdoctors")\
        .map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5]))

    z = sc.cassandraTable("census", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("zipcode", "pop", "meanincome")\
        .map(lambda x: (x[0], x[1], x[2]))

    df_x = sqlContext.createDataFrame(x)
    df_y = sqlContext.createDataFrame(y)
    df_z = sqlContext.createDataFrame(z)
    
    # (zipcode, numofhospitals, zipcode, providernumber, hospitalname, x, y, numberofdoctors) -> (zipcode, numofhospitals, providernumber, hospitalname, x, y, numberofdoctors)
    def x_y_joinSeparator(): return lambda x: (x[0], x[1], x[3], x[4], x[5], x[6], x[7])

    cond = [df_x._1 == df_y._1]
    x_y = df_x.join(df_y, cond)\
			.map(x_y_joinSeparator())

    for row in x_y.collect():
	print(row)

    df_x_y = sqlContext.createDataFrame(x_y)
			
    # (zipcode, numofhospitals, providernumber, hospitalname, x, y, numberofdoctors, zipcode, pop, meanincome)
    #-> (zipcode, numofhospitals, providernumber, hospitalname, x, y, numberofdoctors, pop, meanincome, pop / (numofhospitals * numofdoctors) )
    def x_y_z_joinSeparator(): return lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[8], x[9], float(x[8]) / (x[1] * x[6]))

    cond = [df_x_y._1 == df_z._1]
    popDoctorRatioHospitalRDD = df_x_y.join(df_z, cond) \
			.map(x_y_z_joinSeparator()) \

    customSchema = StructType([ \
    	StructField("ZipCode", StringType(), True), \
	StructField("NumberOfHospitals", IntegerType(), True), \
	StructField("ProviderNumber", StringType(), True), \
    	StructField("HospitalName", StringType(), True), \
	StructField("X", StringType(), True), \
	StructField("Y", StringType(), True), \
	StructField("NumberOfDoctors", IntegerType(), True), \
	StructField("Pop", IntegerType(), True), \
	StructField("MeanIncome", IntegerType(), True), \
	StructField("Ratio", FloatType(), True)])

    popDoctorRatioHospitalDF = sqlContext.createDataFrame(popDoctorRatioHospitalRDD, customSchema)

    dfJSONRDD = popDoctorRatioHospitalDF.toJSON().collect()

    mergedJSON = []
    for row in dfJSONRDD:
        mergedJSON.append(row)

    with open("/home/cs179g/json/popDoctorRatioHospital.json", "wb") as outfile:
        json.dump(mergedJSON, outfile)

    popDoctorRatioHospitalRDD.map(lambda row: {'zipcode': row[0],
				   'numberofhospitals' : row[1],
                                   'providernumber' : row[2],
				   'hospitalname' : row[3],
				   'x' : row[4],
				   'y' : row[5],
                                   'numberofdoctors': row[6],
				   'pop' : row[7],
				   'meanincome' : row[8],
                                   'ratio': row[9]}).saveToCassandra(keyspace='hospitals', table='pop_doctor_ratio_hospital')

    sc.stop()
