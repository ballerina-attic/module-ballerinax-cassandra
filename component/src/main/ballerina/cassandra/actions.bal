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

# The Datatype of the parameter.
public type Type "INT"|"BIGINT"|"VARINT"|"FLOAT"|"DOUBLE"|"TEXT"|"BOOLEAN"|"LIST";

# A 32-bit signed integer.
@final public Type TYPE_INT = "INT";

# A 64-bit signed long.
@final public Type TYPE_BIGINT = "BIGINT";

# Arbitrary precision integer.
@final public Type TYPE_VARINT = "VARINT";

#  A 32-bit IEEE-754 floating point.
@final public Type TYPE_FLOAT = "FLOAT";

# A 64-bit IEEE-754 floating point.
@final public Type TYPE_DOUBLE = "DOUBLE";

# UTF-8 encoded string.
@final public Type TYPE_TEXT = "TEXT";

# Boolean value either True or false.
@final public Type TYPE_BOOLEAN = "BOOLEAN";

# A collection of one or more ordered elements.
@final public Type TYPE_LIST = "LIST";

# Represents complex parameter passed to `select` or `update` operation.

# + cqlType - Cassandra type of the parameter
# + value - Value of the parameter
public type Parameter record {
    Type cqlType;
    any value;
};

# The union type representing either a `Parameter` or a primitive ballerina type.
public type Param string|int|boolean|float|Parameter;

# The Caller Actions for Cassandra databases.
public type CallerActions object {
    # Select data from cassandra datasource.

    # + queryString - Query to be executed
    # + recordType - The Type result should be mapped to
    # + return - `table` representing the result of the select action or `error` if an error occurs
    public extern function select(string queryString, typedesc recordType, Param... parameters)
        returns (table|error);

    # Execute update query on cassandra datasource.

    # + queryString - Query to be executed
    # + return - `nil` or `error` if an error occurs
    public extern function update(string queryString, Param... parameters) returns (error?);

};
