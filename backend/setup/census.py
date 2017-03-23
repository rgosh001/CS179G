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
        print("Usage: census.py <file>", file=sys.stderr)
        exit(-1)
    
    conf = SparkConf().setAppName("Census App").set("spark.dynamicAllocation.enabled", "false")
    sc = CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    
    customSchema = StructType([ \
    	StructField("ZipCode", StringType(), True), \
	StructField("MeanIncome", IntegerType(), True), \
        StructField("Pop", IntegerType(), True)])
    
    censusContext = sqlContext.read.format('com.databricks.spark.csv') \
    	.options(header='true') \
    	.load(sys.argv[1], schema=customSchema)
    
    print('# of zip codes: {}'.format(censusContext.count()))
    temp = censusContext.map(lambda row: {'zipcode': row.ZipCode,
                                    'meanincome': row.MeanIncome,
                                    'pop': row.Pop}).saveToCassandra(keyspace='census', table='test')


    print("--- %s seconds ---" % (time.time() - start_time))

    ###
    #.collect() 
    #sc.parallelize(temp).saveToCassandra(keyspace='hospitals', table='test')
    #sc.saveToCassandra(keyspace='hospitals', table='test')
    ###
