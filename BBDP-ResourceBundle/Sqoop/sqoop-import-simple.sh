##########################################################################
#
#  		Apache Sqoop Examples : Simple Sqoop Import
# 			 Copyright : V2 Maestros LLC @2016-2017
#
##########################################################################

#Install Sqoop from http://www.apache.org/dyn/closer.lua/sqoop/

#The sample data in departments is available in a CSV file in the same directory
#You may import the CSV into a MySQL table first

#Delete target folder in HDFS
hadoop fs -rm -r sqoopdata/departments

#Perform import of table "departments" from retail_db into HDFS
sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username root \
--password cloudera \
--table departments \
-m 1 \
--target-dir /user/cloudera/sqoopdata/departments

#Print the contents of the HDFS folder
hadoop fs -cat /user/cloudera/sqoopdata/departments/*

##########################################################################
