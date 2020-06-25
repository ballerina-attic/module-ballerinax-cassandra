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

import ballerina/java;

# Represents Cassandra client endpoint.
public type Client client object {
    private ClientConfig clientConfig;

    # Gets called when the `Client` is instantiated.
    #
    # + config - Client endpoint configuration
    public function init(ClientConfig config) {
        self.clientConfig = config;
        initClient(self, config);
    }

    # Select data from cassandra datasource.
    #
    # + queryString - Query to be executed
    # + recordType - The Type result should be mapped to
    # + parameters - The parameters to be passed to the select query
    # + return - `table` representing the result of the select action or `error` if an error occurs
    public remote function 'query(string queryString, typedesc<record {|any|error...;|}> recordType,
        Param... parameters) returns table<record {}>|error {
        return externQuery(self, queryString, recordType, parameters);
    }

    # Execute update query on cassandra datasource.
    #
    # + queryString - Query to be executed
    # + parameters - The parameters to be passed to the update query
    # + return - `nil` upon success or `error` if an error occurs
    public remote function update(string queryString, Param... parameters) returns error? {
        return externUpdate(self, queryString, parameters);
    }

    # Stops the registered service.
    public function stop() {
        close(self);
    }
};

function initClient(Client cassandraClient, ClientConfig clientConfig) = @java:Method {
    name: "init",
    class: "org.ballerinalang.cassandra.actions.ExternAction"
} external;

function externUpdate(Client cassandraClient, string queryString, Param[] parameters) returns error? = @java:Method {
    name: "update",
    class: "org.ballerinalang.cassandra.actions.ExternAction"
} external;

function externQuery(Client cassandraClient, string queryString, typedesc<record {|any|error...;|}> recordType,
    Param[] parameters) returns table<record {}>|error = @java:Method {
    name: "query",
    class: "org.ballerinalang.cassandra.actions.ExternAction"
} external;

function close(Client cassandraClient) = @java:Method {
    name: "close",
    class: "org.ballerinalang.cassandra.actions.ExternAction"
} external;
