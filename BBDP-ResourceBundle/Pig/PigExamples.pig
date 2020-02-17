/*##########################################################################
#
#  				Apache Pig Examples
# 			 Copyright : V2 Maestros @2016
#
##########################################################################*/

-- Load the CSV file using a schema into a bag
salesData = LOAD 'SalesData.csv'
	USING PigStorage(',')
	AS (custId:int,
		custName:chararray,
		productType:chararray, 
		value:int);
	
-- Show the structure of the bag 	
DESCRIBE salesData;
-- Dump the contents of the bag
DUMP salesData;
		
--Filter sales data for "Computer" and store in another bag
compSales = FILTER salesData 
				BY productType=='Computer';  
DUMP compSales;
EXPLAIN compSales;

--Create a group by customer name.
groupedSales = GROUP compSales 
				BY custName;
DUMP groupedSales;

--Perform sum of values by customers
custSales = FOREACH groupedSales 
			GENERATE group, SUM(compSales.(value));
DUMP custSales;

--Store the results into another directory.
STORE custSales INTO 'custSales' 
		USING PigStorage(',');
		
-- View the result directory
fs -cat custSales/*;

/******************************************
		Sample for a Join
*******************************************
We load two files - employee and department.
Then we join them by department ID */

-- Load employee Data
empData = LOAD 'EmployeeData.csv'
	USING PigStorage(',')
	AS (empId:int,
		empName:chararray,
		deptId:int,
		gender:chararray);
		
-- Load department data		
deptData = LOAD 'DeptData.csv'
	USING PigStorage(',')
	AS (deptId:int,
		deptName:chararray);

--joined Data		
joinedData = JOIN empData by deptId, 
					deptData by deptId;
					
DUMP joinedData;


/***********************************************
		Statistics with Pig
***********************************************/
REGISTER datafu-1.2.0.jar;
DEFINE Quantile datafu.pig.stats.Quantile('0.0','0.25','0.5','0.75','1.0');

orderedSales = ORDER salesData BY value DESC;
DUMP orderedSales;

quanData = FOREACH ( GROUP orderedSales ALL ) 
			GENERATE Quantile(orderedSales.value);
DUMP quanData;

/***********************************************
		Writing a UDF
***********************************************/
REGISTER maestros.jar;

gender = foreach empData generate empId, 
			maestros.genderString(gender);

DUMP gender;

/***********************************************
		Creating datindicator variables
***********************************************/
empIndData = FOREACH empData GENERATE empId,empName,deptId, 
		( gender == 'M'? 1 : 0) as isMale, 
		(gender=='F' ? 1:0) as isFemale;

DESCRIBE empIndData;
DUMP empIndData;

/***********************************************
		Centering and scaling
***********************************************/
valMean = FOREACH ( GROUP salesData ALL ) 
			GENERATE AVG(salesData.value);
valSD = FOREACH ( GROUP salesData ALL) 
			GENERATE SQRT(datafu.pig.stats.VAR(salesData.value));
tempSales = CROSS salesData, valMean,valSD;

csSales = FOREACH tempSales 
			GENERATE $0 as custId, $1 as custName, $3 as value, 
			($3 - $4)/$5 as CSValue;

/***********************************************
		Binning
***********************************************/
REGISTER maestros.jar;

 binnedSales = FOREACH salesData 
		GENERATE custName, value, 
		maestros.binning(value) as valueBin;
		
