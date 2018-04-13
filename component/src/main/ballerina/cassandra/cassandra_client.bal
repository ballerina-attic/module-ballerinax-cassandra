// Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package ballerina.cassandra;

@Description {value:"Parameter struct represents a query parameter for the queries specified in connector actions."}
@Field {value:"cqlType: The cassandra data type of the corresponding parameter"}
@Field {value:"value: Value of parameter passed into the query"}
public type Parameter {
    Type cqlType,
    any value,
};

@Description {value:"The Datatype of the parameter"}
@Field {value:"INT: A 32-bit signed integer"}
@Field {value:"BIGINT: A 64-bit signed long"}
@Field {value:"VARINT: Arbitrary precision integer"}
@Field {value:"FLOAT: A 32-bit IEEE-754 floating point"}
@Field {value:"DOUBLE: A 64-bit IEEE-754 floating point"}
@Field {value:"TEXT: UTF-8 encoded string"}
@Field {value:"BOOLEAN: Boolean value either True or false"}
@Field {value:"LIST: A collection of one or more ordered elements"}
public type Type
"INT"|"BIGINT"|"VARINT"|"FLOAT"|"DOUBLE"|"TEXT"|"BOOLEAN"|"LIST";

@final public Type TYPE_INT = "INT";
@final public Type TYPE_BIGINT = "BIGINT";
@final public Type TYPE_VARINT = "VARINT";
@final public Type TYPE_FLOAT = "FLOAT";
@final public Type TYPE_DOUBLE = "DOUBLE";
@final public Type TYPE_TEXT = "TEXT";
@final public Type TYPE_BOOLEAN = "BOOLEAN";
@final public Type TYPE_LIST = "LIST";


@Description {value:"The Client Connector for Cassandra database."}
public type CassandraClient object {

@Description {value:"Select data from cassandra datasource."}
@Param {value:"query: Query to be executed"}
@Param {value:"parameters: Parameter array used with the given query"}
@Param {value:"type:The Type result should be mapped to"}
@Return {value:"Result set for the given query"}
public native function select (string queryString, (Parameter[] | ()) parameters, typedesc recordType)
returns (table | error);

@Description {value:"Execute update query on cassandra datasource."}
@Param {value:"query: Query to be executed"}
@Param {value:"parameters: Parameter array used with the given query"}
public native function update (string queryString, (Parameter[] | ()) parameters) returns
(error | ());

@Description {value:"The close action implementation to shutdown the cassandra connections."}
public native function close () returns (error | ());

};

@Description {value:"CassandraConnectorError type represents an error occured during the Cassandra client invocation"}
@Field {value:"message:  An error message explaining about the error"}
@Field {value:"cause: The error(s) that caused CassandraConnectorError to get thrown"}
public type CassandraConnectorError {
    string message,
    error[] cause,
};
