##########################################################################
#
#  		Apache Sqoop Examples : Sqoop Job setup and execute
# 			 Copyright : V2 Maestros,LLC @2016-2017
#
##########################################################################

#The sample data in departments is available in a CSV file in the same directory
#You may import the CSV into a MySQL table first

#Delete target folder in HDFS
hadoop fs -rm -r sqoopdata/departments

#create a password file. Do this once
echo -n "cloudera" > pwdfile.txt

#Create a job that incrementally imports departments table
echo 'Creating a job'
sqoop job --create deptJob \
-- import \
--connect jdbc:mysql://localhost/retail_db \
--username root \
--password-file file:///home/cloudera/pipelines/pwdfile.txt \
--table departments \
-m 1 \
--target-dir /user/cloudera/sqoopdata/departments \
--incremental append \
--check-column department_id

#List current jobs in sqoop
echo 'Listing jobs'
sqoop job --list

#Inspect a specific job
echo 'Inspecting jobs'
sqoop job --show deptJob

#execute a job
sqoop job --exec deptJob

#See the contents of the destination
hadoop fs -cat /user/cloudera/sqoopdata/departments/*

#Insert additional records, re-execute job and see the contents of the destination.

#Delete a job
sqoop job --delete deptJob

