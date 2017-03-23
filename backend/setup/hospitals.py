from __future__ import print_function

import sys
import time
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark_cassandra import CassandraSparkContext


start_time = time.time()
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: station.py <file>", file=sys.stderr)
        exit(-1)
    
    conf = SparkConf().setAppName("Hospitals App").set("spark.dynamicAllocation.enabled", "false")
    sc = CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    
    customSchema = StructType([ \
    	StructField("ProviderNumber", StringType(), True), \
    	StructField("HospitalName", StringType(), True), \
    	StructField("Address", StringType(), True), \
    	StructField("City", StringType(), True), \
    	StructField("State", StringType(), True), \
    	StructField("ZipCode", StringType(), True), \
    	StructField("CountyName", StringType(), True), \
    	StructField("X", FloatType(), True), \
    	StructField("Y", FloatType(), True), \
    	StructField("PhoneNumber", StringType(), True), \
    	StructField("NumberOfDoctors", StringType(), True)])
    
    hospitalsContext = sqlContext.read.format('com.databricks.spark.csv') \
    	.options(header='true') \
    	.load(sys.argv[1], schema=customSchema)
    

    valid_hospitals = hospitalsContext.filter(hospitalsContext.NumberOfDoctors != '')

    print('# of hospitals: {}'.format(valid_hospitals.count()))
    temp = valid_hospitals.map(lambda row: {'providernumber': row.ProviderNumber,
                                    'hospitalname': row.HospitalName,
                                    'address': row.Address,
                                    'city': row.City,
                                    'state': row.State,
                                    'zipcode': row.ZipCode,
                                    'countyname': row.CountyName,
                                    'x': row.X,
                                    'y': row.Y,
                                    'phonenumber': row.PhoneNumber,
                                    'numberofdoctors': row.NumberOfDoctors}).saveToCassandra(keyspace='hospitals', table='test')


print("--- %s seconds ---" % (time.time() - start_time))
    #.collect() 
    #sc.parallelize(temp).saveToCassandra(keyspace='hospitals', table='test')
    #sc.saveToCassandra(keyspace='hospitals', table='test')
