#!/bin/bash

# The following script allows user to benchmark Cassandra performance
# Enter 4 arguments to select the data to benchmark:
# /benchmark.sh arg1 arg2 arg3 arg4

# The arguments can have the following values:
#		arg1: Database - 8, 40
#		arg2: Number of nodes - 1, 3
#		arg3: Number of clients - 10, 20, 40

# Run app
echo -ne "Compiling project..."
mvn -q install &>/dev/null
mvn -q compile
echo "success"

rm -rf log
mkdir log
echo -ne "Execute $3 clients in background..."
for i in `seq $3`; do
    mvn -q exec:java -Dexec.mainClass="app.MainDriver" -Dexec.args="$1 $i" 1> log/output$i.log 2> log/error$i.log &
done
wait
echo "completed"

# Benchmark
