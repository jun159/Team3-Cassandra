#!/bin/bash

# The following script allows user to retrieve data and transactions
# and load the data into the database by running the following:
# Enter 4 arguments to select the data to benchmark:
# /bulkload.sh arg1

# Conditions:
# The argument can have the following values:
#		arg1: Database - 8, 40
#		Make sure cqlsh ?????

declare -r FOLDER_DATA="data"
declare -r FOLDER_D8="D8-data"
declare -r FOLDER_D40="D40-data"

# Create data folder only if not exist
echo -ne "Checking whether data folder exist..."
if [ -d $FOLDER_DATA ]
then 
	echo "yes"
else
	mkdir data
	echo "new folder created successfully"
fi

cd data

# Download D8 database if not exist
echo -ne "Checking whether D8-data exist..."
if [ -d $FOLDER_D8 ]
then 
	echo "yes"
else
	echo "no"
	echo "Start downloading D8-data..."
	wget http://www.comp.nus.edu.sg/~cs4224/D8-data.zip &>/dev/null
	unzip D8-data.zip &>/dev/null
	wget http://www.comp.nus.edu.sg/~cs4224/D8-xact-revised-b.zip &>/dev/null
	unzip D8-xact-revised-b.zip &>/dev/null
	echo "D8-data download completed"
fi

# Download D40 database if not exist
echo -ne "Checking whether D40-data exist..."
if [ -d $FOLDER_D40 ]
then 
	echo "yes"
else
	echo "no"
	echo -ne "Start downloading D40-data..."
#	wget http://www.comp.nus.edu.sg/~cs4224/D40-data.zip &>/dev/null
#	unzip D40-data.zip &>/dev/null
#	wget http://www.comp.nus.edu.sg/~cs4224/D40-xact-revised-b.zip &>/dev/null
#	unzip D40-xact-revised-b.zip &>/dev/null
	echo "D40-data download completed"
fi

cd

# Bulk load data
echo -ne "Loading data into Cassandra..."
cd /temp/datastax-ddc-3.9.0/bin/
./cqlsh -f ~/Team3-Cassandra/schemascript.cql
echo "Successfully loaded all data"
