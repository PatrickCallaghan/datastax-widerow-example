Wide Row example
========================================================

This is a simple example of using the wide row feature in Cassandra. It also shows how to use batching in Cassandra 1.2 and asynchronous requests with the Java driver.

## Running the demo 

You will need a java runtime (preferably 7) along with maven 3 to run this demo. Start DSE 3.1.X or a cassandra 1.2.X instance on your local machine. This demo just runs as a standalone process on the localhost.

This demo uses quite a lot of memory so it is worth setting the MAVEN_OPTS to run maven with more memory

    export MAVEN_OPTS=-Xmx512M

## Schema Setup
Note : This will drop the keyspace "datastax_widerow_demo" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetupSingle"

To create the a multi data center cluster for DSE with a standard Cassandra, Analytics and Solr set up run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetupMulti" 

To run the insert

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.widerow.Main" -DnoOfRows=10 -DnoOfCols=1000
