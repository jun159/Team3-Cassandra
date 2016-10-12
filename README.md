# Team3-Cassandra

## Introduction
Cassandra benchmarking measures the performance of different data modeling with different set of nodes and clients. With comparison of different data modeling, this allows us to find out the optimized database schema design for Cassandra.

## Instructions
### 1. Install Datastax(>=3.9.0)
```
cd /temp 
wget http://downloads.datastax.com/datastax-ddc/datastax-ddc-3.9.0-bin.tar.gz
tar zxvf datastax-ddc-3.9.0-bin.tar.gz
```

### 2. Install Maven(>=3.3.9)
```
wget http://download.nus.edu.sg/mirror/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
tar xzvf apache-maven-3.3.9-bin.tar.gz
export PATH=/temp/apache-maven-3.3.9/bin:$PATH
```

### 3. Configure cassandra server (to run three nodes)
```
cd /temp/datastax-ddc-3.9.0/conf
vim cassandra.yaml
```
Edit the settings in 'cassandra.yaml' file:</br>
     1) seeds: If you are running three nodes, add the IP addresses of the three nodes.</br>
     <p><img style="display: block;
    margin: 0 auto;" align="left" src="https://github.com/jun159/Team3-Cassandra/blob/master/IMG%20CS4224.jpg" width="800"></p>
    
     <br><br><br><br>2) Listen: Add in the IP address of the current node in use.</br>
Save the file and restart the cassandra server.</br>

### 4. Bulkload data
The bulkload.sh script requires 1 argument that represents the type of dataset (D8 or D40). </br>
     a) To bulkload all D8 datasets into the database, run `bash bulkload.sh 8`. </br>
     b) To bulkload all D40 datasets into the database, run `bash bulkload.sh 40`. </br>

### 5. Run benchmark
The benchmark.sh script requires 2 arguments that represents the type of dataset (D8 or D40) and number of clients. </br>
     a) To benchmark D8 datasets with 10 clients, run `bash benchmark.sh 8 10`.</br>
     b) To benchmark D40 datasets with 10 clients, run `bash benchmark.sh 40 10`.</br>

### 6. Stop server when not using
```
ps -ax | grep cassandra //Look for the pid in the output “XXXX pts/0    Sl     0:19 java”
kill XXXX
```

## References
https://maven.apache.org/install.html </br>
http://www.mkyong.com/maven/install-maven-on-mac-osx/ </br>
