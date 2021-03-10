## Table of Contents
- [General Info](#general-info)
- [Technologies](#technologies)
- [Setup](#setup)

## General Info
In this mini-project, my goal was to create a Spark program to produce a report of the total number of accidents per make and per year of the vehicle. The following steps were executed:

- Extract Key (VIN) and Value (vehicle make, vehicle year and incident type) from dataset
- Populate vehicle make and year for records missing these values
- Filter records that are not accident incidents
- Count accident incidents by make and year
- Output results to text file

## Technologies
Mini-project is created with: 
* Python 3.8.3
* Spark 3.0.1

## Setup
To run this mini-project, follow the steps below:

1. ```$ git clone https://github.com/BenGriffith/post-sale-automobile-redesign.git```
2. 
```
$ cd sample
$ spark-submit autoinc_spark.py