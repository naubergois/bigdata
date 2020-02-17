##########################################################################
#
#  			APPLY Project Solutions : Build Big Data Pipelines
# 			 Copyright : V2 Maestros,LLC @2016-2017
#
##########################################################################

Project Requirement:
********************
1. There is a MySQL Database table that contains data about various books.
The information is in a table "books_master".
Data for this table is available in the file "apply_books_master.csv"
Please import this data into the table books_master.
The data about the books need to be imported into MongoDB with one book
as one document called "books_summary". The mongo DB table should have the following fields

- Book Name
- Total Sales count
- Total Sales Amount
- Average Review

2. the MySQL database also has information about books sales. 
The data for this table is available in the file "apply_book_sales.csv"
Please import this data into the table "books_sales"
The books sales data need to be imported and updated to the same MongoDB Table
for sales count and sales amount

3. The book gets reviews from its website. The reviews for various books
are available in flat files. These review ratings need to be moved and
updated in the same mongo db table - for average review rating for each book.
The source log data for this file is available "apply_book_reviews.log"

Build a big data pipeline that will take data from the MySQL database
as well as log files. Create the MongoDB table that has one document 
per book with summaries of sales and reviews.

Note: You can also enhance the problem /solution by addition
incremental imports and creating summaries by month.
##########################################################################

#Setup a customer directory for a specific month
hadoop fs -mkdir pipeline/books

#Create Sqoop job for downloading books master
sqoop job --create booksMasterJob \
-- import \
--connect jdbc:mysql://localhost/retail_db \
--username root \
--password-file file:///home/cloudera/pipelines/pwdfile.txt \
--table books_master \
-m 1 \
--target-dir /user/cloudera/pipeline/books/booksMaster

#Run the job
sqoop job --exec booksMasterJob
#Check the results
hadoop fs -cat /user/cloudera/pipeline/books/booksMaster/*

#Create sqoop job for downloading books sales
sqoop job --create booksSalesJob \
-- import \
--connect jdbc:mysql://localhost/retail_db \
--username root \
--password-file file:///home/cloudera/pipelines/pwdfile.txt \
--table books_sales \
-m 1 \
--target-dir /user/cloudera/pipeline/books/booksSales

#Run the job
sqoop job --exec booksSalesJob
#Check the results
hadoop fs -cat /user/cloudera/pipeline/books/booksSales/*

##########################################################################
#The following is the configuration file for Flume - Books Ratings

# Name the components on this agent
ratingsagent1.sources = filesrc1
ratingsagent1.sinks = hdfssink1
ratingsagent1.channels = memchl1

# Describe/configure the source
ratingsagent1.sources.filesrc1.type = spooldir
ratingsagent1.sources.filesrc1.spoolDir = /home/cloudera/pipelines/bookreviews
ratingsagent1.sources.filesrc1.deletePolicy=immediate

# Describe the sink
ratingsagent1.sinks.hdfssink1.type = hdfs
ratingsagent1.sinks.hdfssink1.hdfs.path=hdfs://localhost:8020/user/cloudera/pipeline/books/bookReviews
ratingsagent1.sinks.hdfssink1.hdfs.fileType=DataStream

# Use a channel which buffers events in memory
ratingsagent1.channels.memchl1.type = memory
ratingsagent1.channels.memchl1.capacity = 1000
ratingsagent1.channels.memchl1.transactionCapacity = 100

# Bind the source and sink to the channel
ratingsagent1.sources.filesrc1.channels = memchl1
ratingsagent1.sinks.hdfssink1.channel = memchl1

#run command
#flume-ng agent --conf conf --conf-file books_flume.conf --name ratingsagent1 -Dflume.root.logger=INFO,console
##########################################################################

#View the copied reviews data
hadoop fs -cat /user/cloudera/pipeline/books/bookReviews/*

#The following are Pig commands to summarize data
customerMaster = LOAD 'pipeline/books/booksMaster'
USING PigStorage(',')
as ( name:chararray, pages:chararray, year:chararray);
			
REGISTER mongo-hadoop-core-2.0.1.jar;
REGISTER mongo-hadoop-pig-2.0.1.jar;
REGISTER mongo-java-driver-3.4.0.jar;

STORE customerMaster INTO 'mongodb://localhost:27017/pipeline.books_summary' USING
 com.mongodb.hadoop.pig.MongoInsertStorage('name');

--Summarize and Load sales data
bookSales = LOAD 'pipeline/books/booksSales'
USING PigStorage(',')
as ( name:chararray, sale_date:chararray, sale_amount:double);

groupedSales = GROUP bookSales by name;

salesSummary = FOREACH groupedSales 
			GENERATE group as name, 
				(double)COUNT( bookSales) as salesCount:double,
				SUM(bookSales.sale_amount) as salesValue:double;
			
STORE salesSummary INTO 'mongodb://localhost:27017/pipeline.books_summary' 
           USING com.mongodb.hadoop.pig.MongoUpdateStorage(
                 '{_id:"\$name"}', --Where
                 '{\$set:{salesCount:"\$salesCount",salesValue:"\$salesValue" }}', --Set
                 'name:chararray, salesCount:double, salesValue:double'
           );

-- Get review data and summarize
custRating = LOAD 'pipeline/books/bookReviews'
USING PigStorage(',')
as ( name:chararray, rating:int);

groupedRating = GROUP custRating by name;

ratingSummary = FOREACH groupedRating 
			GENERATE group as name, 
				AVG(custRating.rating) as rating:double;
			
STORE ratingSummary  INTO 
		'mongodb://localhost:27017/pipeline.books_summary' 
           USING com.mongodb.hadoop.pig.MongoUpdateStorage(
                 '{_id:"\$name"}', --Where
                 '{\$set:{avgRating:"\$rating" }}', 
                 'name:chararray, rating:double'
           );

