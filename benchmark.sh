#!/bin/bash

# The following script allows user to benchmark Cassandra performance
# 1) Enter 4 arguments to select the data to benchmark: 
#		/benchmark.sh arg1 arg2 arg3 arg4

# The arguments can have the following values:
#		arg1: Database - 8, 40
#		arg2: Number of nodes - 1, 3
#		arg3: Number of clients - 10, 20, 40

declare -r FOLDER_DATA="data"

# Create data folder only if not exist
if [ -d $FOLDER_DATA ]
then 
	echo "Folder data found"
else
	mkdir data
	echo "Folder data created"
fi

cd data

# Download D8 database if not exist
echo -ne "Checking whether D8-data exist..."
if [ -f /data/D8-data ]
then 
	echo "yes"
else
	echo "no"
	echo -ne "Start downloading D8-data..."
	wget http://www.comp.nus.edu.sg/~cs4224/D8-data.zip
	unzip D8-data.zip
	curl http://www.comp.nus.edu.sg/~cs4224/D8-xact-revised-b.zip
	unzip D8-xact-revised-b.zip
	echo "completed"
fi

# Download D40 database if not exist
echo -ne "Checking whether D40-data exist..."
if [ -f /data/D40-data ]
then 
	echo "yes"
else
	echo "no"
	echo -ne "Start downloading D8-data..."
	wget http://www.comp.nus.edu.sg/~cs4224/D40-data.zip
	unzip D40-data.zip
	curl http://www.comp.nus.edu.sg/~cs4224/D40-xact-revised-b.zip
	unzip D40-xact-revised-b.zip
	echo "D40-data download completed"
fi

# Bulk load data

# Run app

# Benchmark

