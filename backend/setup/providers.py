from __future__ import print_function

import sys
import time
import re
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark_cassandra import CassandraSparkContext

start_time = time.time()
if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: station.py <file>", file=sys.stderr)
		exit(-1)

	conf = SparkConf().setAppName("Providers App").set("spark.dynamicAllocation.enabled", "false")
	sc = CassandraSparkContext(conf=conf)
	sqlContext = SQLContext(sc)

	customSchema = StructType([\
		StructField("NPI", StringType(), True), \
		StructField("LastName", StringType(), True), \
		StructField("FirstName", StringType(), True), \
		StructField("MiddleName", StringType(), True), \
		StructField("Prefix", StringType(), True), \
		StructField("Suffix", StringType(), True), \
		StructField("Credential", StringType(), True), \
		StructField("OtherLastName", StringType(), True), \
		StructField("OtherFirstName", StringType(), True), \
		StructField("OtherMiddleName", StringType(), True), \
		StructField("BusinessPracticeLocationLine1", StringType(), True), \
		StructField("BusinessPracticeLocationLine2", StringType(), True), \
		StructField("BusinessPracticeLocationCity", StringType(), True), \
		StructField("BusinessPracticeLocationState", StringType(), True), \
		StructField("BusinessPracticeLocationPostalCode", StringType(), True), \
		StructField("BusinessPracticeLocationTelephoneNumber", StringType(), True), \
		StructField("BusinessPracticeLocationFaxNumber", StringType(), True), \
		StructField("Gender", StringType(), True), \
		StructField("TaxonomyCode1", StringType(), True), \
		StructField("TaxonomyCode2", StringType(), True), \
		StructField("TaxonomyCode3", StringType(), True), \
		StructField("TaxonomyCode4", StringType(), True), \
		StructField("TaxonomyCode5", StringType(), True), \
		StructField("TaxonomyCode6", StringType(), True), \
		StructField("TaxonomyCode7", StringType(), True), \
		StructField("MedSchool", StringType(), True), \
		StructField("GradYear", StringType(), True), \
		StructField("MedicareAddress1Line1", StringType(), True), \
		StructField("MedicareAddress1Line2", StringType(), True), \
		StructField("MedicareAddress1City", StringType(), True), \
		StructField("MedicareAddress1State", StringType(), True), \
		StructField("MedicareAddress1ZIPCode", StringType(), True), \
		StructField("MedicareAddress2Line1", StringType(), True), \
		StructField("MedicareAddress2Line2", StringType(), True), \
		StructField("MedicareAddress2City", StringType(), True), \
		StructField("MedicareAddress2State", StringType(), True), \
		StructField("MedicareAddress2ZIPCode", StringType(), True), \
		StructField("MedicareAddress3Line1", StringType(), True), \
		StructField("MedicareAddress3Line2", StringType(), True), \
		StructField("MedicareAddress3City", StringType(), True), \
		StructField("MedicareAddress3State", StringType(), True), \
		StructField("MedicareAddress3ZIPCode", StringType(), True), \
		StructField("MedicareAddress4Line1", StringType(), True), \
		StructField("MedicareAddress4Line2", StringType(), True), \
		StructField("MedicareAddress4City", StringType(), True), \
		StructField("MedicareAddress4State", StringType(), True), \
		StructField("MedicareAddress4ZIPCode", StringType(), True), \
		StructField("HospitalAffiliationCCN1", StringType(), True), \
		StructField("HospitalAffiliationLBN1", StringType(), True), \
		StructField("HospitalAffiliationCCN2", StringType(), True), \
		StructField("HospitalAffiliationLBN2", StringType(), True), \
		StructField("HospitalAffiliationCCN3", StringType(), True), \
		StructField("HospitalAffiliationLBN3", StringType(), True), \
		StructField("HospitalAffiliationCCN4", StringType(), True), \
		StructField("HospitalAffiliationLBN4", StringType(), True), \
		StructField("HospitalAffiliationCCN5", StringType(), True), \
		StructField("HospitalAffiliationLBN5", StringType(), True), \
		StructField("BusinessPracticeLocationX", StringType(), True), \
		StructField("BusinessPracticeLocationY", StringType(), True), \
		StructField("X1", StringType(), True), \
		StructField("Y1", StringType(), True), \
		StructField("X2", StringType(), True), \
		StructField("Y2", StringType(), True), \
		StructField("X3", StringType(), True), \
		StructField("Y3", StringType(), True), \
		StructField("X4", StringType(), True), \
		StructField("Y4", StringType(), True)])

	customSchemaSubset = StructType([\
		StructField("NPI", StringType(), True), \
		StructField("Credential", StringType(), True), \
		StructField("BusinessPracticeLocationLine1", StringType(), True), \
		StructField("BusinessPracticeLocationLine2", StringType(), True), \
		StructField("BusinessPracticeLocationCity", StringType(), True), \
		StructField("BusinessPracticeLocationState", StringType(), True), \
		StructField("BusinessPracticeLocationPostalCode", StringType(), True), \
		StructField("BusinessPracticeLocationTelephoneNumber", StringType(), True), \
		StructField("TaxonomyCode1", StringType(), True)])

	df_a = sqlContext.read.format('com.databricks.spark.csv') \
		.options(header='true') \
		.load(sys.argv[1], schema=customSchema) \
		#.select("NPI", "Credential", "BusinessPracticeLocationLine1", "BusinessPracticeLocationLine2", "BusinessPracticeLocationCity", "BusinessPracticeLocationState", "BusinessPracticeLocationPostalCode", "BusinessPracticeLocationTelephoneNumber", "TaxonomyCode1")

	#providersContext.withColumn("BusinessPracticeLocationPostalCode", providersContext.BusinessPracticeLocationPostalCode if len(providersContext.BusinessPracticeLocationPostalCode) <= 5 else providersContext.BusinessPracticeLocationPostalCode[:5])

        def postalCodeTruncator(): return lambda x: (x[0], x[6], x[10], x[11], x[12], x[13], ( str(x[14])[:5] if (len(str(x[14])) > 5) else x[14] ) , x[15], x[18])

        rdd_a = df_a.map(postalCodeTruncator())#.filter(lambda x: x[6] != '')
	
	#for row in rdd_a:
	#	print (row)

    	providersContext = sqlContext.createDataFrame(rdd_a, customSchemaSubset)

	### https://gist.github.com/samuelsmal/feb86d4bdd9a658c122a706f26ba7e1e#file-pyspark_udf_filtering-py-L4
	def regex_abc(x):
	    regexs = ['[a-zA-Z]']
	    
	    if x and x.strip():
		for r in regexs:
		    if re.match(r, x, re.IGNORECASE):
			return True
	    
	    return False 
	    
	filter_abc = udf(regex_abc, BooleanType())

	def regex_num(x):
	    regexs = ['[0-9]']
	    
	    if x and x.strip():
		for r in regexs:
		    if re.match(r, x, re.IGNORECASE):
			return True
	    
	    return False 
	    
	filter_num = udf(regex_num, BooleanType())
	###

	#print('# of total providers: {}'.format(df_a.count()))
	#print('# of providers: {}'.format(providersContext.count()))
	#for row in temp:
	#	print(row)
	temp = providersContext\
				.filter(\
					col("NPI").isNotNull() &\
					col("Credential").isNotNull() &\
					col("TaxonomyCode1").isNotNull() &\
					col("BusinessPracticeLocationLine1").isNotNull() &\
					col("BusinessPracticeLocationCity").isNotNull() &\
					col("BusinessPracticeLocationState").isNotNull() &\
					col("BusinessPracticeLocationPostalCode").isNotNull() &\
					col("BusinessPracticeLocationTelephoneNumber").isNotNull() &\
					col("BusinessPracticeLocationPostalCode").isNotNull() &\
					filter_abc(col("TaxonomyCode1")) &\
					filter_num(col("BusinessPracticeLocationPostalCode")))\
				.map(lambda row: {\
					'npi': row.NPI, \
					'credential': row.Credential, \
					'businesspracticelocationline1': row.BusinessPracticeLocationLine1, \
					'businesspracticelocationline2': row.BusinessPracticeLocationLine2, \
					'businesspracticelocationcity': row.BusinessPracticeLocationCity, \
					'businesspracticelocationstate': row.BusinessPracticeLocationState, \
					'businesspracticelocationpostalcode': row.BusinessPracticeLocationPostalCode, \
					'businesspracticelocationtelephonenumber': row.BusinessPracticeLocationTelephoneNumber, \
					'taxonomycode1': row.TaxonomyCode1})\
				.saveToCassandra(keyspace='providers', table='test')

print("--- %s seconds ---" % (time.time() - start_time))	
	#.collect() 
	#sc.parallelize(temp).saveToCassandra(keyspace='providers', table='test')
	#temp.saveToCassandra(keyspace='providers',
