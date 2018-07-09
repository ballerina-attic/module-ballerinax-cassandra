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

documentation {
    The Datatype of the parameter.
}
public type Type "INT"|"BIGINT"|"VARINT"|"FLOAT"|"DOUBLE"|"TEXT"|"BOOLEAN"|"LIST";

documentation {
    A 32-bit signed integer.
}
@final public Type TYPE_INT = "INT";

documentation {
    A 64-bit signed long.
}
@final public Type TYPE_BIGINT = "BIGINT";

documentation {
    Arbitrary precision integer.
}
@final public Type TYPE_VARINT = "VARINT";

documentation {
     A 32-bit IEEE-754 floating point.
}
@final public Type TYPE_FLOAT = "FLOAT";

documentation {
    A 64-bit IEEE-754 floating point.
}
@final public Type TYPE_DOUBLE = "DOUBLE";

documentation {
    UTF-8 encoded string.
}
@final public Type TYPE_TEXT = "TEXT";

documentation {
    Boolean value either True or false.
}
@final public Type TYPE_BOOLEAN = "BOOLEAN";

documentation {
    A collection of one or more ordered elements.
}
@final public Type TYPE_LIST = "LIST";

documentation {
    Represents complex parameter passed to `select` or `update` operation.

    F{{cqlType}} Cassandra type of the parameter
    F{{value}} Value of the parameter
}
public type Parameter record {
    Type cqlType,
    any value,
};

documentation {
    The union type representing either a `Parameter` or a primitive ballerina type.
}
public type Param string|int|boolean|float|Parameter;

documentation {
    The Caller Actions for Cassandra databases.
}
public type CallerActions object {
    documentation {
        Select data from cassandra datasource.

        P{{queryString}} Query to be executed
        P{{recordType}} The Type result should be mapped to
        R{{}} `table` representing the result of the select action or `error` if an error occurs
    }
    public native function select(string queryString, typedesc recordType, Param... parameters)
        returns (table|error);

    documentation {
        Execute update query on cassandra datasource.

        P{{queryString}} Query to be executed
        R{{}} `nil` or `error` if an error occurs
    }
    public native function update(string queryString, Param... parameters) returns (error?);

};
