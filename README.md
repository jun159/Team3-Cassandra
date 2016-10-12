# Team3-Cassandra

## Introduction
Cassandra benchmarking measures the performance of different data modeling with different set of nodes and clients. With comparison of different data modeling, this allows us to find out the optimized database schema design for Cassandra.

## Instructions
### 1. Install Datastax(>=3.9.0)
```
cd /temp // Install in temp folder
wget http://downloads.datastax.com/datastax-ddc/datastax-ddc-3.9.0-bin.tar.gz
tar zxvf datastax-ddc-3.9.0-bin.tar.gz
```

### 2. Install Maven(>=3.3.9)
```
cd /temp // Install in temp folder
wget http://download.nus.edu.sg/mirror/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
tar xzvf apache-maven-3.3.9-bin.tar.gz
export PATH=/temp/apache-maven-3.3.9/bin:$PATH
```

### 3. Configuration for three nodes
```
cd /temp/datastax-ddc-3.9.0/conf
vim cassandra.yaml
```
Edit the settings in 'cassandra.yaml' file:

1) seeds: Add the IP addresses of the three nodes.

<img src="https://github.com/jun159/Team3-Cassandra/blob/master/IMG%20CS4224.jpg" height ="200">
    
2) listen_address: Add in the IP address of the current node in use.

<img src="https://github.com/jun159/Team3-Cassandra/blob/master/IMG%202%20CS4224.png" height ="60">

Save the file and restart the cassandra server.

### 4. Download project
Before running the scripts, make sure that the project is in the home folder. Change directory to the project folder to prepare for benchmarking.
```
cd Team3-Cassandra 
```

### 5. Bulkload data
The benchmark.sh script requires 2 arguments that represents the type of dataset (D8 or D40) and number of clients. </br>
a) To bulkload all D8 datasets into the database with 1 node, run `bash bulkload.sh 8 1`. </br>
b) To bulkload all D40 datasets into the database with 3 nodes, run `bash bulkload.sh 40 3`. 

### 6. Run benchmark
The benchmark.sh script requires 2 arguments that represents the type of dataset (D8 or D40) and number of clients. </br>
a) To benchmark D8 datasets with 10 clients, run `bash benchmark.sh 8 10`.</br>
b) To benchmark D40 datasets with 10 clients, run `bash benchmark.sh 40 10`.

### 7. Stop server when not using
```
ps -ax | grep cassandra //Look for the pid in the output “XXXX pts/0    Sl     0:19 java”
kill XXXX
```

## References
https://maven.apache.org/install.html </br>
http://www.mkyong.com/maven/install-maven-on-mac-osx/ 
