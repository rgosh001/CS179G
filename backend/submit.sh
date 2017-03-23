#!/bin/bash

if [ "$#" -eq 0 ]; then
    echo "Proper usage: ./submit.sh [fileName].py ([fileName].csv)"
    exit 1;
fi

if [ "$#" -eq 1 ]; then
	spark-submit --master spark://spark01.cs.ucr.edu:7077 --packages TargetHolding:pyspark-cassandra:0.3.5,com.databricks:spark-csv_2.11:1.5.0 --conf spark.cassandra.connection.host=spark01 $1
fi


if [ "$#" -eq 2 ]; then
	spark-submit --master spark://spark01.cs.ucr.edu:7077 --packages TargetHolding:pyspark-cassandra:0.3.5,com.databricks:spark-csv_2.11:1.5.0 --conf spark.cassandra.connection.host=spark01 $1 $2
fi


