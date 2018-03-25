# Ballerina Cassandra Connector

Ballerina Cassandra Connector is used to connect Ballerina with Cassandra data source. With the Ballerina Cassandra connector Connector following actions are supported.

1. update - To execute a data or schema update query
2. select - To select data from the datasource
3. close - To close the cassandra connection.



Steps to Configure
==================================

Extract ballerina-cassandra-connector-<version>.zip and copy containing jars in to <BRE_HOME>/bre/lib/

Building from the source
==================================
If you want to build Ballerina Cassandra Connector from the source code:

1. Get a clone or download the source from this repository:
    https://github.com/ballerinalang/connector-cassandra
2. Run the following Maven command from the ballerina directory: 
    mvn clean install
3. Copy and extract the distribution created at `/component/target/target/ballerina-cassandra-connector-<version>.zip`  into <BRE_HOME>/bre/lib/.



Sample
==================================

```ballerina
import ballerina/data.cassandra as c;
import ballerina/io;

struct RS {
    int id;
    string name;
    float salary;
}


function main (string[] args) {
    endpoint c:Client conn {
        host:"localhost",
        port:9042,
        username:"cassandra",
        password:"cassandra",
        options:{ 
        queryOptionsConfig:{consistencyLevel:"ONE", defaultIdempotence:false}, 
        protocolOptionsConfig:{sslEnabled:false}, 
        socketOptionsConfig:{connectTimeoutMillis:500, readTimeoutMillis:1000}, 
        poolingOptionsConfig:{maxConnectionsPerHostLocal:5, newConnectionThresholdLocal:10}}
    };

    _ = conn -> update("CREATE KEYSPACE testballerina  WITH replication = {'class':'SimpleStrategy', 
    'replication_factor' : 3}", null);
    io:println("Key space testballerina is created.");

    _ = conn -> update("CREATE TABLE testballerina.person(id int PRIMARY KEY,name text,salary float,income double, 
    married boolean)", null);
    io:println("Table person created.");

    c:Parameter pID = {cqlType:c:Type.INT, value:1};
    c:Parameter pName = {cqlType:c:Type.TEXT, value:"Anupama"};
    c:Parameter pSalary = {cqlType:c:Type.FLOAT, value:100.5};
    c:Parameter pIncome = {cqlType:c:Type.DOUBLE, value:1000.5};
    c:Parameter pMarried = {cqlType:c:Type.BOOLEAN, value:true};
    c:Parameter[] pUpdate = [pID, pName, pSalary, pIncome, pMarried];
    _ = conn -> update("INSERT INTO testballerina.person(id, name, salary, income, married) values (?,?,?,?,?)",
                pUpdate);
    io:println("Insert One Row to Table person.");

    c:Parameter[] paramsSelect1 = [pID];
    table dt1 =? conn -> select("select id, name, salary from testballerina.person where id = ?",
    paramsSelect1, typeof RS);
    while (dt1.hasNext()) {
        var rs =? <RS> dt1.getNext();
        int id = rs.id;
        string name = rs.name;
        float salary = rs.salary;
        io:println("Person:" + rs.id + "|" + rs.name + "|" + rs.salary);
    }

    c:Parameter[] paramsSelect2 = [pID, pName];
    table dt2 =? conn -> select("select id, name, salary from testballerina.person where id = ? and name = ? 
    ALLOW FILTERING", paramsSelect2, typeof RS);
    var j =? <json>dt2;
    io:println(j);

    c:Parameter[] paramsSelect3 = [pSalary];
    table dt3 =? conn -> select("select id, name, salary from testballerina.person where salary = ? ALLOW FILTERING",
    paramsSelect3, typeof RS);
    var x =? <xml>dt3;
    io:println(x);

    _ = conn -> update("DROP KEYSPACE testballerina", null);
    io:println("KEYSPACE testballerina dropped.");

    _ = conn -> close();
    io:println("Connection closed.");
}
 ```
