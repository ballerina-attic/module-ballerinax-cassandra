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
import ballerina/cassandra as c;
import ballerina/io;

type RS {
    int id,
    string name,
    float salary,
};


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
    'replication_factor' : 3}");
    io:println("Key space testballerina is created.");

    _ = conn -> update("CREATE TABLE testballerina.person(id int PRIMARY KEY,name text,salary float,income double, 
    married boolean)");
    io:println("Table person created.");

    c:Parameter pID = (c:TYPE_INT, 1);
    c:Parameter pName = (c:TYPE_TEXT, "Anupama");
    c:Parameter pSalary = (c:TYPE_FLOAT, 100.5);
    c:Parameter pIncome = (c:TYPE_DOUBLE, 1000.5);
    c:Parameter pMarried = (c:TYPE_BOOLEAN, true);
    _ = conn -> update("INSERT INTO testballerina.person(id, name, salary, income, married) values (?,?,?,?,?)",
                pID, pName, pSalary, pIncome, pMarried);
    io:println("Insert One Row to Table person.");

    var temp1 = conn -> select("select id, name, salary from testballerina.person where id = ?", RS, pID);
    table dt1 = check temp1;
    while (dt1.hasNext()) {
        var rs = check <RS> dt1.getNext();
        int id = rs.id;
        string name = rs.name;
        float salary = rs.salary;
        io:println("Person:" + rs.id + "|" + rs.name + "|" + rs.salary);
    }

    var temp2 = conn -> select("select id, name, salary from testballerina.person where id = ? and name = ?
    ALLOW FILTERING", RS, pID, pName);
    table dt2 = check temp2;
    var j = check <json>dt2;
    io:println(j);

    var temp3 = conn -> select("select id, name, salary from testballerina.person where salary = ? ALLOW FILTERING", RS, pSalary);
    table dt3 = check temp3;
    var x = check <xml>dt3;
    io:println(x);

    _ = conn -> update("DROP KEYSPACE testballerina");
    io:println("KEYSPACE testballerina dropped.");

    _ = conn -> close();
    io:println("Connection closed.");
}
 ```