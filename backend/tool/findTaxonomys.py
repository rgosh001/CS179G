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
        
    conf = SparkConf().setAppName("Tool App").setMaster("spark://spark01.cs.ucr.edu:7077")
    sc = CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # pulling data from cassandra

    taxonomyRDD = sc.cassandraTable("providers", "test", row_format=pyspark_cassandra.RowFormat.TUPLE).select("taxonomycode1")

    customSchema = StructType([StructField("TaxonomyCode1", StringType(), True)])

    taxonomyDF = sqlContext.createDataFrame(taxonomyRDD, customSchema).distinct()

    with open("/home/cs179g/logs/taxonomyList", "w") as outfile:
    	for row in taxonomyDF.rdd.collect():
	    outfile.write(row[0] + '\n')

    sc.stop()
