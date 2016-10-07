#!/bin/bash

# The following script allows user to retrieve data and transactions
# and load the data into the database by running the following:
# bash bulkload.sh arg0

# The argument can have the following values:
#		arg0: Database - 8, 40

# Conditions:
#       Project (Team3-Cassandra) is in home directory
#		cqlsh is within /temp/datastax-ddc-3.9.0/bin directory

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
if [ $1 == 8 ]
then
    echo -ne "Checking whether D8-data exist..."
    if [ -d $FOLDER_D8 ]
    then
        echo "yes"
    else
        echo "no"
        echo "Start downloading D8-data..."
        wget http://www.comp.nus.edu.sg/~cs4224/D8-data.zip
        unzip D8-data.zip
        wget http://www.comp.nus.edu.sg/~cs4224/D8-xact-revised-b.zip
        unzip D8-xact-revised-b.zip
        mv D8-xact-revised-b D8-xact
        echo "D8-data download completed"
    fi
fi

# Download D40 database if not exist
if [ $1 == 40 ]
then
    echo -ne "Checking whether D40-data exist..."
    if [ -d $FOLDER_D40 ]
    then
        echo "yes"
    else
        echo "no"
        echo -ne "Start downloading D40-data..."
        wget http://www.comp.nus.edu.sg/~cs4224/D40-data.zip
        unzip D40-data.zip &>/dev/null
        wget http://www.comp.nus.edu.sg/~cs4224/D40-xact-revised-b.zip
        unzip D40-xact-revised-b.zip
        mv D40-xact-revised-b D8-xact
        echo "D40-data download completed"
    fi
fi

cd

# Bulk load data
echo -ne "\nLoading warehouse, district, customer, order, orderline and stock data into Cassandra..."
cd /temp/datastax-ddc-3.9.0/bin
./cqlsh -f ~/Team3-Cassandra/schema.cql
if [ $1 == 8 ]
then
    ./cqlsh -f ~/Team3-Cassandra/schemascript8.cql
else
    ./cqlsh -f ~/Team3-Cassandra/schemascript40.cql
fi

cd ~/Team3-Cassandra
echo -ne "\nLoading item data into Cassandra.."
mvn -q install &>/dev/null
mvn -q compile &>/dev/null
mvn -q exec:java -Dexec.mainClass="database.Denormalize" -Dexec.args="$1"
