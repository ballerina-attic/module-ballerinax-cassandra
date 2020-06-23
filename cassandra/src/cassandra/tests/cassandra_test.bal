// Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/io;
import ballerina/test;

type Person record {
    int id;
    string name;
    float salary;
    float income;
};

Client conn = new ({
    host: "localhost",
    port: 9042,
    username: "cassandra",
    password: "cassandra",
    options: {
        queryOptionsConfig: {consistencyLevel: "ONE", defaultIdempotence: false},
        protocolOptionsConfig: {sslEnabled: false},
        socketOptionsConfig: {connectTimeoutMillis: 500, readTimeoutMillis: 1000},
        poolingOptionsConfig: {maxConnectionsPerHostLocal: 5, newConnectionThresholdLocal: 10}
    }
});

@test:BeforeSuite
function beforeSuiteFunc() {
    var result = conn->update("CREATE KEYSPACE testballerina  WITH replication = " +
        "{'class':'SimpleStrategy','replication_factor' : 3}");
    handleUpdate(result, "Keyspace testballerina creation");
    result = conn->update("CREATE TABLE testballerina.person(id int PRIMARY KEY,name text,salary " +
        "float,income double,married boolean)");
    handleUpdate(result, "Table person creation");
}

@test:Config {}
function test_update_values() {
    Parameter pID = {cqlType: TYPE_INT, value: 4};
    Parameter pName = {cqlType: TYPE_TEXT, value: "Bob"};
    Parameter pSalary = {cqlType: TYPE_FLOAT, value: 100.5};
    Parameter pIncome = {cqlType: TYPE_DOUBLE, value: 1000.5};
    Parameter pMarried = {cqlType: TYPE_BOOLEAN, value: true};
    var result = conn->update("INSERT INTO testballerina.person(id, name, salary, income, married) values (?,?,?,?,?)",
        pID, pName, pSalary, pIncome, pMarried);
    handleUpdate(result, "Insert row 1 to Table person");
}

@test:Config {
    dependsOn: ["test_update_values"]
}
function test_select_values() {
    var result = conn->selectData("select * from testballerina.person", Person);
    if (result is table<Person>) {
        foreach var row in result {
            io:println(row);
        }
    } else {
        handleUpdate(error("SELECT_ERROR", message = "Error in select statement"));
    }
}

@test:AfterSuite
function afterSuiteFunc() {
    var result = conn->update("DROP KEYSPACE testballerina");
    handleUpdate(result, "Drop keyspace testballerina");
    conn.stop();
}

function handleUpdate(()|error result, string message = "") {
    if (result is ()) {
        io:println(message + " success ");
    } else {
        test:assertFail(msg = <string>result.message());
    }
}
