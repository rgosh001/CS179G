#!/bin/bash

./submit.sh query/popDoctorRatioQuery.py && ./submit.sh query/popDoctorRatioHospitalQuery.py && ./submit.sh query/taxonomyCountQuery.py && ./submit.sh query/taxonomyCountHospitalQuery.py
