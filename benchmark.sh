#!/bin/bash

# The following script allows user to benchmark Cassandra performance
# 1) Enter 4 arguments to select the data to benchmark: 
#		/benchmark.sh arg1 arg2 arg3 arg4

# The arguments can have the following values:
#		arg1: Database - 8, 40
#		arg2: Number of nodes - 1, 3
#		arg3: Number of clients - 10, 20, 40

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
	echo -ne "Start downloading D8-data..."
	wget http://www.comp.nus.edu.sg/~cs4224/D40-data.zip &>/dev/null
	unzip D40-data.zip &>/dev/null
	wget http://www.comp.nus.edu.sg/~cs4224/D40-xact-revised-b.zip &>/dev/null
	unzip D40-xact-revised-b.zip &>/dev/null
	echo "D40-data download completed"
fi

cd ../

# Bulk load data
echo -ne "Compiling project..."
mvn -q install &>/dev/null
mvn -q compile
echo "success"
mvn -q exec:java -Dexec.mainClass="database.InsertTables"
mvn -q exec:java -Dexec.mainClass="database.ImportDataModified"

# Run app
rm -rf log
mkdir log
echo -ne "Execute $3 clients in background..."
for i in `seq $3`; do
    mvn -q exec:java -Dexec.mainClass="app.MainDriver" -Dexec.args="$1 $i" 1> log/output$i.log 2> log/error$i.log &
done
wait
echo "completed"

# Benchmark

