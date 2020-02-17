##########################################################################
#
#  		Building Big Data Pipelines - Use Case 2 : Code
# 			 Copyright : V2 Maestros @2016
#
##########################################################################

##########################################################################
#The following commands are to setup 2 Sqoop jobs

#Setup a customer directory for a specific month
hadoop fs -mkdir pipeline/customerDEC16

#Create a job to copy all customer master data
sqoop job --create customerMasterJob \
-- import \
--connect jdbc:mysql://localhost/pipeline \
--username root \
--password-file file:///home/cloudera/pipelines/pwdfile.txt \
--table customer \
-m 1 \
--target-dir /user/cloudera/pipeline/customerDEC16/custMaster \
--incremental lastmodified \
--check-column MODIFIED_TIMESTAMP

#Run the job
sqoop job --exec customerMasterJob
#Check the results
hadoop fs -cat /user/cloudera/pipeline/customerDEC16/custMaster/*

#Create a job to copy all customer sales data
sqoop job --create customerSalesJob \
-- import \
--connect jdbc:mysql://localhost/pipeline \
--username root \
--password-file file:///home/cloudera/pipelines/pwdfile.txt \
--table customer_sales \
-m 1 \
--target-dir /user/cloudera/pipeline/customerDEC16/custSales \
--incremental append \
--check-column SALES_ID

#Run the job
sqoop job --exec customerSalesJob

#Check the results
hadoop fs -cat /user/cloudera/pipeline/customerDEC16/custSales/*

##########################################################################
#The following is the configuration file for Flume - Web Access

# Name the components on this agent
logagent1.sources = filesrc1
logagent1.sinks = hdfssink1
logagent1.channels = memchl1

# Describe/configure the source
logagent1.sources.filesrc1.type = spooldir
logagent1.sources.filesrc1.spoolDir = /home/cloudera/pipelines/accessLog
logagent1.sources.filesrc1.deletePolicy=immediate

# Describe the sink
logagent1.sinks.hdfssink1.type = hdfs
logagent1.sinks.hdfssink1.hdfs.path=hdfs://localhost:8020/user/cloudera/pipeline/customerDEC16/accessLog
logagent1.sinks.hdfssink1.hdfs.fileType=DataStream

# Use a channel which buffers events in memory
logagent1.channels.memchl1.type = memory
logagent1.channels.memchl1.capacity = 1000
logagent1.channels.memchl1.transactionCapacity = 100

# Bind the source and sink to the channel
logagent1.sources.filesrc1.channels = memchl1
logagent1.sinks.hdfssink1.channel = memchl1

#run command
#flume-ng agent --conf conf --conf-file uc3-accessLog.conf --name logagent1 -Dflume.root.logger=INFO,console


##########################################################################
#The following is the configuration file for Flume - Twitter Access

# Name the components on this agent
twitteragent1.sources = filesrc1
twitteragent1.sinks = hdfssink1
twitteragent1.channels = memchl1

# Describe/configure the source
twitteragent1.sources.filesrc1.type = spooldir
twitteragent1.sources.filesrc1.spoolDir = /home/cloudera/pipelines/twitterLog
twitteragent1.sources.filesrc1.deletePolicy=immediate

# Describe the sink
twitteragent1.sinks.hdfssink1.type = hdfs
twitteragent1.sinks.hdfssink1.hdfs.path=hdfs://localhost:8020/user/cloudera/pipeline/customerDEC16/twitterLog
twitteragent1.sinks.hdfssink1.hdfs.fileType=DataStream

# Use a channel which buffers events in memory
twitteragent1.channels.memchl1.type = memory
twitteragent1.channels.memchl1.capacity = 1000
twitteragent1.channels.memchl1.transactionCapacity = 100

# Bind the source and sink to the channel
twitteragent1.sources.filesrc1.channels = memchl1
twitteragent1.sinks.hdfssink1.channel = memchl1

#run command
#flume-ng agent --conf conf --conf-file uc3-twitterLog.conf --name twitteragent1 -Dflume.root.logger=INFO,console


##########################################################################
#The following is the Pig commands to integrate data

%declare MONTH_PREFIX 'DEC2016';

customerMaster = LOAD 'pipeline/customerDEC16/custMaster'
USING PigStorage(',')
as ( id:int, name:chararray, email:chararray, timestamp:chararray, twitter:chararray);

custMonth = FOREACH customerMaster GENERATE
			CONCAT ('$MONTH_PREFIX','_', (chararray)id ) as custKey,
			id as custId, '$MONTH_PREFIX' as rMonth,
			name as custName;
			
REGISTER mongo-hadoop-core-2.0.1.jar;
REGISTER mongo-hadoop-pig-2.0.1.jar;
REGISTER mongo-java-driver-3.4.0.jar;

STORE custMonth INTO 'mongodb://localhost:27017/pipeline.cust_summary' USING
 com.mongodb.hadoop.pig.MongoInsertStorage('custKey');

 --Summarize and Load sales data
custSales = LOAD 'pipeline/customerDEC16/custSales'
USING PigStorage(',')
as ( id:int, custid:int, product:chararray, price:double, salesdate:chararray);

joinedSales = JOIN customerMaster by id, custSales by custid;

groupedSales = GROUP joinedSales by custid;

salesSummary = FOREACH groupedSales 
			GENERATE CONCAT ('$MONTH_PREFIX','_',(chararray)group) as custKey, 
				(double)COUNT( joinedSales) as salesCount:double,
				SUM(joinedSales.price) as salesValue:double;
			
STORE salesSummary INTO 'mongodb://localhost:27017/pipeline.cust_summary' 
           USING com.mongodb.hadoop.pig.MongoUpdateStorage(
                 '{_id:"\$custKey"}', --Where
                 '{\$set:{salesCount:"\$salesCount",salesValue:"\$salesValue" }}', --Set
                 'custKey:chararray, salesCount:double, salesValue:double'
           );

 --Summarize and Load access data
custAccess = LOAD 'pipeline/customerDEC16/accessLog'
USING PigStorage(',')
as ( accessTS:chararray, email:chararray, page:chararray, result:int);

joinedAccess = JOIN customerMaster by email, custAccess by email;

groupedAccess = GROUP joinedAccess by id;

accessSummary = FOREACH groupedAccess 
			GENERATE CONCAT ('$MONTH_PREFIX','_',(chararray)group) as custKey, 
				(double)COUNT( joinedAccess) as accessCount:double;
			
STORE accessSummary INTO 'mongodb://localhost:27017/pipeline.cust_summary' 
           USING com.mongodb.hadoop.pig.MongoUpdateStorage(
                 '{_id:"\$custKey"}', --Where
                 '{\$set:{accessCount:"\$accessCount"}}', --Set
                 'custKey:chararray, accessCount:double'
           );

 --Summarize and Load tweet data
custTweet = LOAD 'pipeline/customerDEC16/twitterLog'
USING PigStorage(',')
as ( tweetTS:chararray, handle:chararray, tweet:chararray);

joinedTweet = JOIN customerMaster by twitter, custTweet by handle;

groupedTweet = GROUP joinedTweet by id;

tweetSummary = FOREACH groupedTweet 
			GENERATE CONCAT ('$MONTH_PREFIX','_',(chararray)group) as custKey, 
				(double)COUNT( joinedTweet) as tweetCount:double;
			
STORE tweetSummary INTO 'mongodb://localhost:27017/pipeline.cust_summary' 
           USING com.mongodb.hadoop.pig.MongoUpdateStorage(
                 '{_id:"\$custKey"}', --Where
                 '{\$set:{tweetCount:"\$tweetCount"}}', --Set
                 'custKey:chararray, tweetCount:double'
           );
		   
