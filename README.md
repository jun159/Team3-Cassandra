# Team3-Cassandra

### Intro

### Instructions
#### 1. Install Datastax(>=3.9)
```
cd /temp 
wget http://downloads.datastax.com/datastax-ddc/datastax-ddc-3.9.0-bin.tar.gz
tar zxvf datastax-ddc-3.9.0-bin.tar.gz
wget http://download.nus.edu.sg/mirror/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
tar xzvf apache-maven-3.3.9-bin.tar.gz
export PATH=/temp/apache-maven-3.3.9/bin:$PATH
```

#### 2. Install Maven(>=3.9.9)

#### 3. Configure cassandra server

#4. Run script
Run `bash benchmark.sh 8 10`
Run `bash benchmark.sh 8 10`

#### 5.Remember to stop server when not using
Look for the pid in the output “XXXX pts/0    Sl     0:19 java”:
```
ps -ax | grep cassandra 
kill XXXX
```

### References
https://maven.apache.org/install.html </br>
http://www.mkyong.com/maven/install-maven-on-mac-osx/ </br>