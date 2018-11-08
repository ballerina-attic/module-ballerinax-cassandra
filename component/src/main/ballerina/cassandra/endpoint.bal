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
public type Client object {
        public ClientEndpointConfiguration clientEndpointConfig;
        public CallerActions callerActions;

    # Gets called when the endpoint is being initialized during the module initialization.
    public function init(ClientEndpointConfiguration config) {
        self.callerActions = createClient(config);
    }

    # Returns the connector that client code uses.
    #
    # + return - The connector that client code uses
    public function getCallerActions() returns CallerActions {
        return self.callerActions;
    }

    # Stops the registered service.
    public function stop() {
        close(self.callerActions);
    }
};

# An internal function used by clients to shutdown the connection pool.
#
# + callerActions - CallerActions object that encapsulates the connection/connection pool
public extern function close(CallerActions callerActions);

extern function createClient(ClientEndpointConfiguration clientEndpointConfig) returns CallerActions;

# The Client endpoint configuration for SQL databases.
#
# + host - The host of the database to connect
# + port - The port of the database to connect
# + username - Username for the database connection
# + password - Password for the database connection
# + options - Properties for the connection configuration
public type ClientEndpointConfiguration record {
    string host;
    int port;
    string username;
    string password;
    ConnectionProperties options;
};

# ConnectionProperties type represents the properties which are used to configure Cassandra connection.
#
# + clusterName - The name of the cluster object
# + loadBalancingPolicy - The policy that decides which Cassandra hosts to contact for each new query
# + reconnectionPolicy - The policy that schedules reconnection attempts to a node
# + retryPolicy - The policy that defines a default behavior to adopt when a request fails
# + dataCenter - The data center used with DCAwareRoundRobinPolicy
# + withoutMetrics - Disables metrics collection for the created cluster if true
# + withoutJMXReporting - Disables JMX reporting of the metrics if true
# + allowRemoteDCsForLocalConsistencyLevel - Determine whether to allow DCAwareRoundRobinPolicy to return remote
#   hosts when building query plans for queries having consistency level LOCAL_ONE or LOCAL_QUORUM
# + constantReconnectionPolicyDelay - The constant wait time between reconnection attempts of
#   ConstantReconnectionPolicy
# + exponentialReconnectionPolicyBaseDelay - The base delay in milliseconds for ExponentialReconnectionPolicy
#   The maximum delay in milliseconds between reconnection attempts of ExponentialReconnectionPolicy
# + queryOptionsConfig - Options related to defaults for individual queries
# + poolingOptionsConfig - Options related to connection pooling
# + socketOptionsConfig - Options to configure low-level socket options for the connections kept to the Cassandra
#   hosts
# + protocolOptionsConfig - Options of the Cassandra native binary protocol
public type ConnectionProperties record {
    string clusterName;
    string loadBalancingPolicy;
    string reconnectionPolicy;
    string retryPolicy;
    string dataCenter;

    boolean withoutMetrics;
    boolean withoutJMXReporting;
    boolean allowRemoteDCsForLocalConsistencyLevel;

    int constantReconnectionPolicyDelay = -1;
    int exponentialReconnectionPolicyBaseDelay = -1;
    int exponentialReconnectionPolicyMaxDelay = -1;

    QueryOptionsConfiguration queryOptionsConfig;
    PoolingOptionsConfiguration poolingOptionsConfig;
    SocketOptionsConfiguration socketOptionsConfig;
    ProtocolOptionsConfiguration protocolOptionsConfig;
};

# Options of the Cassandra native binary protocol.
#
# + sslEnabled - Enables the use of SSL for the created cluster
# + noCompact - Whether or not to include the NO_COMPACT startup option
# + maxSchemaAgreementWaitSeconds - The maximum time to wait for schema agreement before returning from a DDL query
# + initialProtocolVersion - Version of the native protocol supported by the driver
# + compression - Compression supported by the Cassandra binary protocol
public type ProtocolOptionsConfiguration record {
    boolean sslEnabled;
    boolean noCompact;

    int maxSchemaAgreementWaitSeconds = -1;

    string initialProtocolVersion;
    string compression;
};

# Options related to defaults for individual queries.
#
# + consistencyLevel - Determines how many nodes in the replica must respond for the coordinator node to
#   successfully process a non-lightweight transaction. Supported values are ANY, ONE, TWO, THREE, QUORUM,
#   ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE
# + serialConsistencyLevel - The serial consistency level is only used by conditional updates
#   (INSERT, UPDATE or DELETE statements with an IF condition). For those, the serial consistency level
#   defines the consistency level of the serial phase (or 'paxos' phase) while the normal consistency level
#   defines the consistency for the 'learn' phase. Supported values are SERIAL, LOCAL_SERIAL
# + defaultIdempotence - A statement is idempotent if it can be applied multiple times without changing the
#   result beyond the initial application
# + metadataEnabled - Toggles client-side token and schema metadata
#   enablement
# + reprepareOnUp - Determines whether the driver should re-prepare all cached prepared statements on a host
#   when it marks it back up
# + prepareOnAllHosts - Determines whether the driver should prepare statements on all hosts in the cluster
# + fetchSize - Sets the default fetch size to use for SELECT queries
# + maxPendingRefreshNodeListRequests - Determines the maximum number of node list refresh requests that
#   the control connection can accumulate before executing them
# + maxPendingRefreshNodeRequests - Determines the maximum number of node refresh requests that the control
#   connection can accumulate before executing them
# + maxPendingRefreshSchemaRequests - Determines the maximum number of schema refresh requests that the
#   control connection can accumulate before executing them
# + refreshNodeListIntervalMillis - Determines the default window size in milliseconds used to debounce node
#   list refresh requests
# + refreshNodeIntervalMillis - Determines the default window size in milliseconds used to debounce node
#   refresh requests
# + refreshSchemaIntervalMillis - Determines the default window size in milliseconds used to debounce schema refresh
#   requests
public type QueryOptionsConfiguration record {
    string consistencyLevel;
    string serialConsistencyLevel;

    boolean defaultIdempotence;
    boolean metadataEnabled = true;
    boolean reprepareOnUp = true;
    boolean prepareOnAllHosts = true;

    int fetchSize = -1;
    int maxPendingRefreshNodeListRequests = -1;
    int maxPendingRefreshNodeRequests = -1;
    int maxPendingRefreshSchemaRequests = -1;
    int refreshNodeListIntervalMillis = -1;
    int refreshNodeIntervalMillis = -1;
    int refreshSchemaIntervalMillis = -1;
};


# Options related to connection pooling.
#
# + maxRequestsPerConnectionLocal - The maximum number of requests per connection for local hosts
# + maxRequestsPerConnectionRemote - The maximum number of requests per connection for remote hosts
# + idleTimeoutSeconds - The timeout before an idle connection is removed
# + poolTimeoutMillis - The timeout when trying to acquire a connection from a host's pool
# + maxQueueSize - The maximum number of requests that get enqueued if no connection is available
# + heartbeatIntervalSeconds - The heart beat interval, after which a message is sent on an idle connection
#   to make sure it's still alive
# + coreConnectionsPerHostLocal - The core number of connections per local host
# + maxConnectionsPerHostLocal - The maximum number of connections per local host
# + newConnectionThresholdLocal - The threshold that triggers the creation of a new connection to a local host
# + coreConnectionsPerHostRemote - The core number of connections per remote host
# + maxConnectionsPerHostRemote - The maximum number of connections per remote host
# + newConnectionThresholdRemote - The threshold that triggers the creation of a new connection to a remote host
public type PoolingOptionsConfiguration record {
    int maxRequestsPerConnectionLocal = -1;
    int maxRequestsPerConnectionRemote = -1;
    int idleTimeoutSeconds = -1;
    int poolTimeoutMillis = -1;
    int maxQueueSize = -1;
    int heartbeatIntervalSeconds = -1;
    int coreConnectionsPerHostLocal = -1;
    int maxConnectionsPerHostLocal = -1;
    int newConnectionThresholdLocal = -1;
    int coreConnectionsPerHostRemote = -1;
    int maxConnectionsPerHostRemote = -1;
    int newConnectionThresholdRemote = -1;
};

# Options to configure low-level socket options for the connections kept to the Cassandra hosts.
#
# + connectTimeoutMillis - The connection timeout in milliseconds
# + readTimeoutMillis - The per-host read timeout in milliseconds
# + soLinger - The linger-on-close timeout
# + receiveBufferSize - A hint to the size of the underlying buffers for incoming network I/O
# + sendBufferSize - A hint to the size of the underlying buffers for outgoing network I/O
public type SocketOptionsConfiguration record {
    int connectTimeoutMillis = -1;
    int readTimeoutMillis = -1;
    int soLinger = -1;
    int receiveBufferSize = -1;
    int sendBufferSize = -1;
};

