[![Build Status](https://travis-ci.org/wso2-ballerina/module-cassandra.svg?branch=master)](https://travis-ci.org/wso2-ballerina/module-cassandra)

# Ballerina Cassandra Client Endpoint

Ballerina Cassandra Client Endpoint is used to connect Ballerina with Cassandra data source. With the Ballerina Cassandra client endpoint following actions are supported.

1. update - To execute a data or schema update query
2. select - To select data from the datasource
3. close - To close the cassandra connection.

Steps to Configure
==================================

Extract wso2-cassandra-<version>.zip and copy containing jars in to <BRE_HOME>/bre/lib/

Building from the source
==================================
If you want to build Ballerina Cassandra Connector from the source code:

1. Get a clone or download the source from this repository:
    https://github.com/wso2-ballerina/module-cassandra
2. Run the following Maven command from the ballerina directory: 
    mvn clean install
3. Extract the distribution created at `/component/target/wso2-cassandra-<version>.zip`. Run the install.{sh/bat} script to install the module.
You can uninstall the module by running uninstall.{sh/bat}.

Sample
==================================

```ballerina
import wso2/cassandra as c;
import ballerina/io;

type Person record {
    int id;
    string name;
    float salary;
};

public function main() {
    c:Client conn = new({
        host: "localhost",
        port: 9042,
        username: "cassandra",
        password: "cassandra",
        options: {
            queryOptionsConfig: { consistencyLevel: "ONE", defaultIdempotence: false },
            protocolOptionsConfig: { sslEnabled: false },
            socketOptionsConfig: { connectTimeoutMillis: 500, readTimeoutMillis: 1000 },
            poolingOptionsConfig: { maxConnectionsPerHostLocal: 5, newConnectionThresholdLocal: 10 } }
    });

    var returned = conn->update("CREATE KEYSPACE testballerina  WITH replication = {'class':'SimpleStrategy',
                      'replication_factor' : 3}");
    handleUpdate(returned, "Keyspace testballerina creation");

    returned = conn->update("CREATE TABLE testballerina.person(id int PRIMARY KEY,name text,salary float,income double,
                      married boolean)");
    handleUpdate(returned, "Table person creation");

    c:Parameter pID = { cqlType: c:TYPE_INT, value: 1 };
    c:Parameter pName = { cqlType: c:TYPE_TEXT, value: "Anupama" };
    c:Parameter pSalary = { cqlType: c:TYPE_FLOAT, value: 100.5 };
    c:Parameter pIncome = { cqlType: c:TYPE_DOUBLE, value: 1000.5 };
    c:Parameter pMarried = { cqlType: c:TYPE_BOOLEAN, value: true };
    returned = conn->update("INSERT INTO testballerina.person(id, name, salary, income, married) values (?,?,?,?,?)",
        pID, pName, pSalary, pIncome, pMarried);
    handleUpdate(returned, "Insert One Row to Table person");

    var selectRet = conn->select("select id, name, salary from testballerina.person where id = ?", Person, pID);

    if (selectRet is table<Person>) {
        foreach row in selectRet {
            io:println("Person:" + row.id + "|" + row.name + "|" + row.salary);
        }
    } else if (selectRet is error) {
        io:println("Select data from person table failed: " + <string>selectRet.detail().message);
    }

    selectRet = conn->select("select id, name, salary from testballerina.person where id = ? and name = ?
                                    ALLOW FILTERING", Person, pID, pName);

    if (selectRet is table) {
        var jsonRet = <json>selectRet;
        if (jsonRet is json) {
            io:print("JSON: ");
            io:println(io:sprintf("%s", jsonRet));
        } else {
            io:println("Error in table to json conversion");
        }
    } else if (selectRet is error) {
        io:println("Select data from person table failed: " + <string>selectRet.detail().message);
    }

    selectRet = conn->select("select id, name, salary from testballerina.person where salary = ? ALLOW FILTERING",
        Person, pSalary);
    if (selectRet is table) {
        var xmlRet = <xml>selectRet;
        if (xmlRet is xml) {
            io:print("XML: ");
            io:println(io:sprintf("%s", xmlRet));
        } else {
            io:println("Error in table to xml conversion");
        }
    } else if (selectRet is error) {
        io:println("Select data from person table failed: " + <string>selectRet.detail().message);
    }

    returned = conn->update("DROP KEYSPACE testballerina");
    handleUpdate(returned, "Drop keyspace testballerina");

    conn.stop();
    io:println("Connection closed.");
}

function handleUpdate(()|error returned, string message) {
    if (returned is ()) {
        io:println(message + " success ");
    } else if (returned is error) {
        io:println(message + " failed: " + <string>returned.detail().message);
    }
}
 ```
