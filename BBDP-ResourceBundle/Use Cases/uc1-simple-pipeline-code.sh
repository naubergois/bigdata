##########################################################################
#
#  		Building Big Data Pipelines - Use Case 1 : Code
# 			 Copyright : V2 Maestros @2016-2017
#
##########################################################################

##########################################################################
#The following are commands for Sqoop. They can either be executed on the
#shell or through a shell script

#create the destination directory once
hadoop fs -mkdir pipeline
hadoop fs -mkdir archive

#Create a Sqoop job to incrementally copy records
sqoop job --create auditTrailJob \
-- import \
--connect jdbc:mysql://localhost/pipeline \
--username root \
--password-file file:///home/cloudera/pipelines/pwdfile.txt \
--table audit_trail \
-m 1 \
--target-dir /user/cloudera/pipeline/uc1-audit-trail \
--incremental append \
--check-column id

#Run the job
sqoop job --exec auditTrailJob

hadoop fs -cat /user/cloudera/pipeline/uc1-audit-trail/*

##########################################################################
#The following are commands for Mongo DB.

use pipeline;
db.createCollection("audit_trail");
 
##########################################################################
#The following are commands for Pig. 

auditData = LOAD '/user/cloudera/pipeline/uc1-audit-trail'
USING PigStorage(',')
as ( id:int, eventdate:chararray, user:chararray, action:chararray);

REGISTER mongo-hadoop-core-2.0.1.jar;
REGISTER mongo-hadoop-pig-2.0.1.jar;
REGISTER mongo-java-driver-3.4.0.jar;

STORE auditData INTO 'mongodb://localhost:27017/pipeline.audit_trail' USING
 com.mongodb.hadoop.pig.MongoInsertStorage('');
 
##########################################################################
#The following are commands for shell script 

#Archive processed records. Move records to archive directory
TODATE=`date +%Y%m%d`
hadoop fs -mkdir archive/$TODATE
hadoop fs -mv pipeline/uc1-audit-trail/* archive/$TODATE/uc1-audit-trail
 
 