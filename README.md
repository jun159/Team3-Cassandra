# Team3-Cassandra

How to install Datastax?

cd /temp
wget http://downloads.datastax.com/datastax-ddc/datastax-ddc-3.9.0-bin.tar.gz
tar zxvf datastax-ddc-3.9.0-bin.tar.gz
wget http://download.nus.edu.sg/mirror/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
tar xzvf apache-maven-3.3.9-bin.tar.gz
export PATH=/temp/apache-maven-3.3.9/bin:$PATH

https://maven.apache.org/install.html
http://www.mkyong.com/maven/install-maven-on-mac-osx/


Remember to stop server when not using
ps -ax | grep cassandra
[Look for the pid in the output “XXXX pts/0    Sl     0:19 java”]
kill XXXX

scp the D8-data folder from local to remote server
cd to the file location
example : scp -r D8-data a0108235@xcnd6:~/
scp schema and copy_scripts from local to remote server
scp Team3-Cassandra from local to remote server
Run the schema.cql (running this query: SOURCE ‘~/schema.cql’) in cqlsh follow by copy_scripts (running this query: SOURCE ‘~/copy_scripts’)
cd Team3-Cassandra folder
use Maven to install, compile and execute:
mvn install
mvn compile
mvn exec:java -Dexec.mainClass=”app.MainDriver”
If Maven cannot run, then go set the path for Maven.
If you install maven in the main folder
export PATH=~/apache-maven-3.3.9/bin:$PATH

