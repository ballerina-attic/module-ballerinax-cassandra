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
    Represents Cassandra client endpoint.
}
public type Client object {
        public ClientEndpointConfiguration clientEndpointConfig;
        public CallerActions callerActions;

    documentation {
        Gets called when the endpoint is being initialized during the package initialization.
    }
    public function init(ClientEndpointConfiguration config) {
        self.callerActions = createClient(config);
    }

    documentation {
        Returns the connector that client code uses.

        R{{}} The connector that client code uses
    }
    public function getCallerActions() returns CallerActions {
        return self.callerActions;
    }

    documentation {
        Stops the registered service.
    }
    public function stop() {
        close(self.callerActions);
    }
};

documentation {
    An internal function used by clients to shutdown the connection pool.

    P{{callerActions}} CallerActions object that encapsulates the connection/connection pool
}
public native function close(CallerActions callerActions);

native function createClient(ClientEndpointConfiguration clientEndpointConfig) returns CallerActions;

documentation {
    The Client endpoint configuration for SQL databases.

    F{{host}} The host of the database to connect
    F{{port}} The port of the database to connect
    F{{username}} Username for the database connection
    F{{password}} Password for the database connection
    F{{options}} Properties for the connection configuration
}
public type ClientEndpointConfiguration record {
    string host,
    int port,
    string username,
    string password,
    ConnectionProperties options,
};

documentation {
    ConnectionProperties type represents the properties which are used to configure Cassandra connection.

    F{{clusterName}} The name of the cluster object
    F{{loadBalancingPolicy}} The policy that decides which Cassandra hosts to contact for each new query
    F{{reconnectionPolicy}} The policy that schedules reconnection attempts to a node
    F{{retryPolicy}} The policy that defines a default behavior to adopt when a request fails
    F{{dataCenter}} The data center used with DCAwareRoundRobinPolicy
    F{{withoutMetrics}} Disables metrics collection for the created cluster if true
    F{{withoutJMXReporting}} Disables JMX reporting of the metrics if true
    F{{allowRemoteDCsForLocalConsistencyLevel}} Determine whether to allow DCAwareRoundRobinPolicy to return remote
    hosts when building query plans for queries having consistency level LOCAL_ONE or LOCAL_QUORUM
    F{{constantReconnectionPolicyDelay}} The constant wait time between reconnection attempts of
    ConstantReconnectionPolicy
    F{{exponentialReconnectionPolicyBaseDelay}} The base delay in milliseconds for ExponentialReconnectionPolicy
    The maximum delay in milliseconds between reconnection attempts of ExponentialReconnectionPolicy
    F{{queryOptionsConfig}} Options related to defaults for individual queries
    F{{poolingOptionsConfig}} Options related to connection pooling
    F{{socketOptionsConfig}} Options to configure low-level socket options for the connections kept to the Cassandra
    hosts
    F{{protocolOptionsConfig}} Options of the Cassandra native binary protocol
}
public type ConnectionProperties record {
    string clusterName,
    string loadBalancingPolicy,
    string reconnectionPolicy,
    string retryPolicy,
    string dataCenter,

    boolean withoutMetrics,
    boolean withoutJMXReporting,
    boolean allowRemoteDCsForLocalConsistencyLevel,

    int constantReconnectionPolicyDelay = -1,
    int exponentialReconnectionPolicyBaseDelay = -1,
    int exponentialReconnectionPolicyMaxDelay = -1,

    QueryOptionsConfiguration queryOptionsConfig,
    PoolingOptionsConfiguration poolingOptionsConfig,
    SocketOptionsConfiguration socketOptionsConfig,
    ProtocolOptionsConfiguration protocolOptionsConfig,
};

documentation {
    Options of the Cassandra native binary protocol.

    F{{sslEnabled}} Enables the use of SSL for the created cluster
    F{{noCompact}} Whether or not to include the NO_COMPACT startup option
    F{{maxSchemaAgreementWaitSeconds}} The maximum time to wait for schema agreement before returning from a DDL query
    F{{initialProtocolVersion}} Version of the native protocol supported by the driver
    F{{compression}} Compression supported by the Cassandra binary protocol
}
public type ProtocolOptionsConfiguration record {
    boolean sslEnabled,
    boolean noCompact,

    int maxSchemaAgreementWaitSeconds = -1,

    string initialProtocolVersion,
    string compression,
};

documentation {
    Options related to defaults for individual queries.

    F{{consistencyLevel}} Determines how many nodes in the replica must respond for the coordinator node to
    successfully process a non-lightweight transaction. Supported values are ANY, ONE, TWO, THREE, QUORUM,
    ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE
    F{{serialConsistencyLevel}} The serial consistency level is only used by conditional updates
    (INSERT, UPDATE or DELETE statements with an IF condition). For those, the serial consistency level
    defines the consistency level of the serial phase (or 'paxos' phase) while the normal consistency level
     defines the consistency for the 'learn' phase. Supported values are SERIAL, LOCAL_SERIAL
    F{{defaultIdempotence}} A statement is idempotent if it can be applied multiple times without changing the
    result beyond the initial application
    F{{metadataEnabled}} Toggles client-side token and schema metadata
    enablement
    F{{reprepareOnUp}} Determines whether the driver should re-prepare all cached prepared statements on a host
    when it marks it back up
    F{{prepareOnAllHosts}} Determines whether the driver should prepare statements on all hosts in the cluster
    F{{fetchSize}} Sets the default fetch size to use for SELECT queries
    F{{maxPendingRefreshNodeListRequests}} Determines the maximum number of node list refresh requests that
    the control connection can accumulate before executing them
    F{{maxPendingRefreshNodeRequests}} Determines the maximum number of node refresh requests that the control
    connection can accumulate before executing them
    F{{maxPendingRefreshSchemaRequests}} Determines the maximum number of schema refresh requests that the
    control connection can accumulate before executing them
    F{{refreshNodeListIntervalMillis}} Determines the default window size in milliseconds used to debounce node
    list refresh requests
    F{{refreshNodeIntervalMillis}} Determines the default window size in milliseconds used to debounce node
    refresh requests
    F{{refreshSchemaIntervalMillis}} Determines the default window size in milliseconds used to debounce schema refresh
    requests
}
public type QueryOptionsConfiguration record {
    string consistencyLevel,
    string serialConsistencyLevel,

    boolean defaultIdempotence,
    boolean metadataEnabled = true,
    boolean reprepareOnUp = true,
    boolean prepareOnAllHosts = true,

    int fetchSize = -1,
    int maxPendingRefreshNodeListRequests = -1,
    int maxPendingRefreshNodeRequests = -1,
    int maxPendingRefreshSchemaRequests = -1,
    int refreshNodeListIntervalMillis = -1,
    int refreshNodeIntervalMillis = -1,
    int refreshSchemaIntervalMillis = -1,
};


documentation {
    Options related to connection pooling.

    F{{maxRequestsPerConnectionLocal}} The maximum number of requests per connection for local hosts
    F{{maxRequestsPerConnectionRemote}} The maximum number of requests per connection for remote hosts
    F{{idleTimeoutSeconds}} The timeout before an idle connection is removed
    F{{poolTimeoutMillis}} The timeout when trying to acquire a connection from a host's pool
    F{{maxQueueSize}} The maximum number of requests that get enqueued if no connection is available
    F{{heartbeatIntervalSeconds}} The heart beat interval, after which a message is sent on an idle connection
    to make sure it's still alive
    F{{coreConnectionsPerHostLocal}} The core number of connections per local host
    F{{maxConnectionsPerHostLocal}} The maximum number of connections per local host
    F{{newConnectionThresholdLocal}} The threshold that triggers the creation of a new connection to a local host
    F{{coreConnectionsPerHostRemote}} The core number of connections per remote host
    F{{maxConnectionsPerHostRemote}} The maximum number of connections per remote host
    F{{newConnectionThresholdRemote}} The threshold that triggers the creation of a new connection to a remote host
}
public type PoolingOptionsConfiguration record {
    int maxRequestsPerConnectionLocal = -1,
    int maxRequestsPerConnectionRemote = -1,
    int idleTimeoutSeconds = -1,
    int poolTimeoutMillis = -1,
    int maxQueueSize = -1,
    int heartbeatIntervalSeconds = -1,
    int coreConnectionsPerHostLocal = -1,
    int maxConnectionsPerHostLocal = -1,
    int newConnectionThresholdLocal = -1,
    int coreConnectionsPerHostRemote = -1,
    int maxConnectionsPerHostRemote = -1,
    int newConnectionThresholdRemote = -1,
};

documentation {
    Options to configure low-level socket options for the connections kept to the Cassandra hosts.

    F{{connectTimeoutMillis}} The connection timeout in milliseconds
    F{{readTimeoutMillis}} The per-host read timeout in milliseconds
    F{{soLinger}} The linger-on-close timeout
    F{{receiveBufferSize}} A hint to the size of the underlying buffers for incoming network I/O
    F{{sendBufferSize}} A hint to the size of the underlying buffers for outgoing network I/O
}
public type SocketOptionsConfiguration record {
    int connectTimeoutMillis = -1,
    int readTimeoutMillis = -1,
    int soLinger = -1,
    int receiveBufferSize = -1,
    int sendBufferSize = -1,
};

