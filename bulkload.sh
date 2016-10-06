#!/bin/bash

# The following script allows user to retrieve data and transactions
# and load the data into the database by running the following:
# bash bulkload.sh

# Conditions:
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
mv D8-xact-revised-b D8-xact
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
#	mv D40-xact-revised-b D8-xact
echo "D40-data download completed"
fi

cd

# Bulk load data
echo "Loading data into Cassandra..."
cd /temp/datastax-ddc-3.9.0/bin
./cqlsh -f ~/Team3-Cassandra/schemascript.cql

cd ~/Team3-Cassandra
echo -ne "Loading stock and item database into Cassandra.."
mvn -q install &>/dev/null
mvn -q compile &>/dev/null
mvn -q exec:java -Dexec.mainClass="database.Denormalize" -Dexec.args="$1" &>/dev/null
echo "success"
