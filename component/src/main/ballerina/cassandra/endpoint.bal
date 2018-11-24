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

# Represents Cassandra client endpoint.
public type Client client object {
    private ClientEndpointConfig clientEndpointConfig;

    # Gets called when the Client is instantiated.
    public function __init(ClientEndpointConfig config) {
        self.clientEndpointConfig = config;
        initClient(self, config);
    }

    # Select data from cassandra datasource.

    # + queryString - Query to be executed
    # + recordType - The Type result should be mapped to
    # + return - `table` representing the result of the select action or `error` if an error occurs
    public remote extern function select(string queryString, typedesc recordType, Param... parameters)
       returns (table|error);

    # Execute update query on cassandra datasource.

    # + queryString - Query to be executed
    # + return - `nil` or `error` if an error occurs
    public remote extern function update(string queryString, Param... parameters) returns (error?);

    # Stops the registered service.
    public function stop() {
        close(self);
    }
};

# An internal function used by clients to shutdown the connection pool.
#
# + cassandraClient - Client object that encapsulates the connection/connection pool
extern function close(Client cassandraClient);

extern function initClient(Client cassandraClient, ClientEndpointConfig clientEndpointConfig);
