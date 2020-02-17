/*##########################################################################
#
#  				Apache Pig Practice Solutions
# 			 Copyright : V2 Maestros @2016
#
##########################################################################*/

--Load the auto-data file.

autoData = LOAD 'auto-data.csv'
	USING PigStorage(',')
	AS (make:chararray,
		hp:int,
		price:int);
		
DESCRIBE autoData;

DUMP autoData;

-- Filter data where HP > 60

filteredAuto = FILTER autoData 
				BY hp > 60 ;  
				
--Create a group by make.
groupedAuto = GROUP filteredAuto 
				BY make;
DUMP groupedAuto;

--Perform AVG of Price
autoMetrics = FOREACH groupedAuto 
			GENERATE group, AVG(filteredAuto.(price));
DUMP autoMetrics;