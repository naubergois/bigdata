##########################################################################
#
#  		Building Big Data Pipelines - Use Case 2 : Code
# 			 Copyright : V2 Maestros @2016
#
##########################################################################

##########################################################################
#The following is the configuration file for Flume

# Name the components on this agent
logagent1.sources = filesrc1
logagent1.sinks = hdfssink1
logagent1.channels = memchl1

# Describe/configure the source
logagent1.sources.filesrc1.type = spooldir
logagent1.sources.filesrc1.spoolDir = /home/cloudera/pipelines/srcdir
logagent1.sources.filesrc1.deletePolicy=immediate

# Describe the sink
logagent1.sinks.hdfssink1.type = hdfs
logagent1.sinks.hdfssink1.hdfs.path=hdfs://localhost:8020/user/cloudera/pipeline/accesslog
logagent1.sinks.hdfssink1.hdfs.fileType=DataStream

# Use a channel which buffers events in memory
logagent1.channels.memchl1.type = memory
logagent1.channels.memchl1.capacity = 1000
logagent1.channels.memchl1.transactionCapacity = 100

# Bind the source and sink to the channel
logagent1.sources.filesrc1.channels = memchl1
logagent1.sinks.hdfssink1.channel = memchl1

#run command
#flume-ng agent --conf conf --conf-file uc2-access-log-processing.conf --name logagent1 -Dflume.root.logger=INFO,console

##########################################################################
#The following are Pig Commands for processing hdfs data

--Load up the log files
logData = LOAD 'pipeline/accesslog'
USING PigStorage(',')
as ( logString:chararray);

--Convert message into individual parts
REGISTER building-pipelines-0.0.1-SNAPSHOT.jar

LogTuple = FOREACH logData GENERATE 
	com.v2maestros.bbdp.pig.BreakupLogMessage(logString) AS
		tuple(clientIp:chararray, logTimestamp:chararray,
			action:chararray, url:chararray,
			responseCode:Int, responseSize:int);

--Extract values from the tuple
LogValues= FOREACH LogTuple GENERATE 
				tuple_0.clientIp, tuple_0.logTimestamp,
				tuple_0.action, tuple_0.url,
				tuple_0.responseCode, tuple_0.responseSize;
	
-- Send data to mongo DB 
REGISTER mongo-hadoop-core-2.0.1.jar;
REGISTER mongo-hadoop-pig-2.0.1.jar;
REGISTER mongo-java-driver-3.4.0.jar;

STORE LogValues INTO 'mongodb://localhost:27017/pipeline.access_log' USING
 com.mongodb.hadoop.pig.MongoInsertStorage('');


