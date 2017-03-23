CREATE KEYSPACE providers WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'3'};

CREATE KEYSPACE hospitals WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'3'};

CREATE KEYSPACE census WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'3'};

CREATE TABLE providers.test (
	NPI varchar PRIMARY KEY,
	Credential varchar,
	BusinessPracticeLocationLine1 varchar,
	BusinessPracticeLocationLine2 varchar,
	BusinessPracticeLocationCity varchar,
	BusinessPracticeLocationState varchar,
	BusinessPracticeLocationPostalCode varchar,
	BusinessPracticeLocationTelephoneNumber varchar,
	TaxonomyCode1 varchar
);

CREATE TABLE providers.taxonomy_count (
	ZipCode varchar,	
	Pop int,	
	MeanIncome int,
	TaxonomyCode1 varchar,
	Count int,
	PRIMARY KEY(ZipCode, TaxonomyCode1)
);

CREATE TABLE providers.taxonomy_count_hospital (
	ZipCode varchar,	
	Pop int,	
	MeanIncome int,
	TaxonomyCode1 varchar,
	ProviderNumber varchar,
	HospitalName varchar,
	X varchar,
	Y varchar,
	Count int,
	NumberOfHospitals int,
	PRIMARY KEY(ZipCode, TaxonomyCode1, ProviderNumber)
);

CREATE TABLE hospitals.test (
	ProviderNumber varchar PRIMARY KEY,
	HospitalName varchar,
	Address varchar,
	City varchar,
	State varchar,
	ZipCode varchar,
	CountyName varchar,
	X float,
	Y float,
	PhoneNumber varchar,
	NumberOfDoctors int
);

CREATE TABLE census.test (
	ZipCode varchar PRIMARY KEY,
	MeanIncome int,
	Pop int
);

CREATE TABLE hospitals.pop_doctor_ratio (
	ZipCode varchar PRIMARY KEY,
	Pop int,
	MeanIncome int,
	NumberOfDoctors int,
	Ratio float
);

CREATE TABLE hospitals.pop_doctor_ratio_hospital (
	ZipCode varchar,
	NumberOfHospitals int,
	ProviderNumber varchar,
	HospitalName varchar,
	X varchar,
	Y varchar,
	NumberOfDoctors int,
	Pop int,	
	MeanIncome int,
	Ratio float,
	PRIMARY KEY(ZipCode, ProviderNumber)
);

CREATE INDEX on providers.test(businesspracticelocationpostalcode);

