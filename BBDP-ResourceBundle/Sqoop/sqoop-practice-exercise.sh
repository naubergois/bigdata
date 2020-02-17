##########################################################################
#
#  		Apache Sqoop Practice Exercise : Sqoop Job setup and execute
# 			 Copyright : V2 Maestros @2016
#
##########################################################################

#The sample data in orders is available in a CSV file in the same directory
#You may import the CSV into a MySQL table first

#create a password file. Do this once
echo -n "cloudera" > pwdfile.txt

#Create a job that incrementally imports orders table
echo 'Creating a job'
sqoop job --create ordersJob \
-- import \
--connect jdbc:mysql://localhost/retail_db \
--username root \
--password-file file:///home/cloudera/pipelines/pwdfile.txt \
--table orders \
-m 1 \
--target-dir /user/cloudera/sqoopdata/orders \
--incremental append \
--check-column order_id

#List current jobs in sqoop
echo 'Listing jobs'
sqoop job --list

#Inspect a specific job
echo 'Inspecting jobs'
sqoop job --show ordersJob

#execute a job
sqoop job --exec ordersJob

#See the contents of the destination
hadoop fs -cat /user/cloudera/sqoopdata/orders/*



