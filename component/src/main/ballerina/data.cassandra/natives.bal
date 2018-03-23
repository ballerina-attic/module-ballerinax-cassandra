package ballerina.data.cassandra;

@Description {value:"Parameter struct represents a query parameter for the queries specified in connector actions."}
@Field {value:"cqlType: The cassandra data type of the corresponding parameter"}
@Field {value:"value: Value of parameter passed into the query"}
public struct Parameter {
    Type cqlType;
    any value;
}

@Description {value:"ConnectionProperties structs represents the properties which are used to configure cassandra
connection"}
@Field {value:"clusterName: The name of the cluster object"}
@Field {value:"loadBalancingPolicy: The policy that decides which Cassandra hosts to contact for each new query"}
@Field {value:"reconnectionPolicy: The policy that schedules reconnection attempts to a node"}
@Field {value:"retryPolicy: The policy that defines a default behavior to adopt when a request fails"}
@Field {value:"dataCenter: The data center used with DCAwareRoundRobinPolicy"}
@Field {value:"withoutMetrics: Disables metrics collection for the created cluster if true"}
@Field {value:"withoutJMXReporting: Disables JMX reporting of the metrics if true"}
@Field {value:"allowRemoteDCsForLocalConsistencyLevel: Determine whether to allow DCAwareRoundRobinPolicy to return
remote hosts when building query plans for queries having consistency level LOCAL_ONE or LOCAL_QUORUM"}
@Field {value:"constantReconnectionPolicyDelay: The constant wait time between reconnection attempts of
ConstantReconnectionPolicy"}
@Field {value:"exponentialReconnectionPolicyBaseDelay: The base delay in milliseconds for
ExponentialReconnectionPolicy"}
@Field {value:"The maximum delay in milliseconds between reconnection attempts of ExponentialReconnectionPolicy"}
@Field {value:"queryOptionsConfig: Options related to defaults for individual queries"}
@Field {value:"poolingOptionsConfig: Options related to connection pooling"}
@Field {value:"socketOptionsConfig: Options to configure low-level socket options for the connections kept to the
Cassandra hosts"}
@Field {value:"protocolOptionsConfig: Options of the Cassandra native binary protocol"}
public struct ConnectionProperties {
    string clusterName;
    string loadBalancingPolicy;
    string reconnectionPolicy;
    string retryPolicy;
    string dataCenter;

    boolean withoutMetrics = false;
    boolean withoutJMXReporting = false;
    boolean allowRemoteDCsForLocalConsistencyLevel = false;

    int constantReconnectionPolicyDelay = -1;
    int exponentialReconnectionPolicyBaseDelay = -1;
    int exponentialReconnectionPolicyMaxDelay = -1;

    QueryOptionsConfiguration queryOptionsConfig;
    PoolingOptionsConfiguration poolingOptionsConfig;
    SocketOptionsConfiguration socketOptionsConfig;
    ProtocolOptionsConfiguration protocolOptionsConfig;
}

@Description {value:"Options of the Cassandra native binary protocol"}
@Field {value:"sslEnabled: Enables the use of SSL for the created Cluster"}
@Field {value:"noCompact: Whether or not to include the NO_COMPACT startup option"}
@Field {value:"maxSchemaAgreementWaitSeconds: The maximum time to wait for schema agreement before returning from a
DDL query"}
@Field {value:"initialProtocolVersion: Version of the native protocol supported by the driver"}
@Field {value:"compression: Compression supported by the Cassandra binary protocol"}
public struct ProtocolOptionsConfiguration {
    boolean sslEnabled;
    boolean noCompact;

    int maxSchemaAgreementWaitSeconds = -1;

    string initialProtocolVersion;
    string compression;
}

@Description {value:"Options related to defaults for individual queries"}
@Field {value:"consistencyLevel: Determines how many nodes in the replica must respond for the coordinator node to
successfully process a non-lightweight transaction. Supported values are ANY, ONE, TWO, THREE, QUORUM,
 ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE"}
@Field {value:"serialConsistencyLevel: The serial consistency level is only used by conditional updates
(INSERT, UPDATE or DELETE statements with an IF condition). For those, the serial consistency level
defines the consistency level of the serial phase (or 'paxos' phase) while the normal consistency level
defines the consistency for the 'learn' phase. Supported values are SERIAL, LOCAL_SERIAL"}
@Field {value:"defaultIdempotence: A statement is idempotent if it can be applied multiple times without changing the
result beyond the initial application"}
@Field {value:"metadataEnabled: Toggles client-side token and schema metadata enablement"}
@Field {value:"reprepareOnUp: Determines whether the driver should re-prepare all cached prepared statements on a host
when it marks it back up"}
@Field {value:"prepareOnAllHosts: Determines whether the driver should prepare statements on all hosts in the cluster"}
@Field {value:"fetchSize:Sets the default fetch size to use for SELECT queries"}
@Field {value:"maxPendingRefreshNodeListRequests: Determines the maximum number of node list refresh requests that
the control connection can accumulate before executing them"}
@Field {value:"maxPendingRefreshNodeRequests: Determines the maximum number of node refresh requests that the control
connection can accumulate before executing them"}
@Field {value:"maxPendingRefreshSchemaRequests: Determines the maximum number of schema refresh requests that the
control connection can accumulate before executing them"}
@Field {value:"refreshNodeListIntervalMillis: Determines the default window size in milliseconds used to debounce node
list refresh requests"}
@Field {value:"refreshNodeIntervalMillis: Determines the default window size in milliseconds used to debounce node
refresh requests"}
@Field {value:"refreshSchemaIntervalMillis: Determines the default window size in milliseconds used to debounce node
list refresh requests"}
public struct QueryOptionsConfiguration {
    string consistencyLevel;
    string serialConsistencyLevel;

    boolean defaultIdempotence = false;
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
}

@Description {value:"Options related to connection pooling"}
@Field {value:"maxRequestsPerConnectionLocal: The maximum number of requests per connection for local hosts"}
@Field {value:"maxRequestsPerConnectionRemote: The maximum number of requests per connection for remote hosts"}
@Field {value:"idleTimeoutSeconds: The timeout before an idle connection is removed"}
@Field {value:"poolTimeoutMillis: The timeout when trying to acquire a connection from a host's pool"}
@Field {value:"maxQueueSize: The maximum number of requests that get enqueued if no connection is available"}
@Field {value:"heartbeatIntervalSeconds: The heart beat interval, after which a message is sent on an idle connection
to make sure it's still alive"}
@Field {value:"coreConnectionsPerHostLocal: The core number of connections per local host"}
@Field {value:"maxConnectionsPerHostLocal: The maximum number of connections per local host"}
@Field {value:"newConnectionThresholdLocal: The threshold that triggers the creation of a new connection to a
local host"}
@Field {value:"coreConnectionsPerHostRemote: The core number of connections per remote host"}
@Field {value:"maxConnectionsPerHostRemote: The maximum number of connections per remote host"}
@Field {value:"newConnectionThresholdRemote: The threshold that triggers the creation of a new connection to a
remote host"}
public struct PoolingOptionsConfiguration {
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
}

@Description {value:"Options to configure low-level socket options for the connections kept to the Cassandra hosts"}
@Field {value:"connectTimeoutMillis: The connection timeout in milliseconds"}
@Field {value:"readTimeoutMillis: The per-host read timeout in milliseconds"}
@Field {value:"soLinger: The linger-on-close timeout"}
@Field {value:"receiveBufferSize: A hint to the size of the underlying buffers for incoming network I/O"}
@Field {value:"sendBufferSize: A hint to the size of the underlying buffers for outgoing network I/O"}
public struct SocketOptionsConfiguration {
    int connectTimeoutMillis = -1;
    int readTimeoutMillis = -1;
    int soLinger = -1;
    int receiveBufferSize = -1;
    int sendBufferSize = -1;

    // TODO: Driver do not set these by default. It takes the default values of the underlying netty transport.
    // But if we take the inputs as booleans and if the value set in the bal file is "false", we wouldn't know
    // if it was set by the user or not. So need to decide how to handle this.
    // boolean keepAlive;
    // boolean reuseAddress;
    // boolean tcpNoDelay;
}

@Description {value:"The Datatype of the parameter"}
@Field {value:"INT: A 32-bit signed integer"}
@Field {value:"BIGINT: A 64-bit signed long"}
@Field {value:"VARINT: Arbitrary precision integer"}
@Field {value:"FLOAT: A 32-bit IEEE-754 floating point"}
@Field {value:"DOUBLE: A 64-bit IEEE-754 floating point"}
@Field {value:"TEXT: UTF-8 encoded string"}
@Field {value:"BOOLEAN: Boolean value either True or false"}
@Field {value:"LIST: A collection of one or more ordered elements"}
public enum Type {
    INT,
    BIGINT,
    VARINT,
    FLOAT,
    DOUBLE,
    TEXT,
    BOOLEAN,
    LIST
}

@Description {value:"Cassandra client connector."}
@Field {value:"host: Comma separated list of addresses of Cassandra nodes which are used to discover the cluster
topology"}
@Field {value:"port: The port configured for the Cassandra cluster"}
@Field {value:"username: The username to connect to a authentication enabled Cassandra cluster"}
@Field {value:"password: The password to connect to a authentication enabled Cassandra cluster"}
@Field {value:"options: Optional properties for cassandra connection"}
public struct ClientConnector {
    string host;
    int port;
    string username;
    string password;
    ConnectionProperties options;
}

@Description {value:"Select data from cassandra datasource."}
@Param {value:"query: Query to be executed"}
@Param {value:"parameters: Parameter array used with the given query"}
@Param {value:"type:The Type result should be mapped to"}
@Return {value:"Result set for the given query"}
public native function <ClientConnector client> select (string queryString, Parameter[] parameters, typedesc structType)
returns (table | CassandraConnectorError);

@Description {value:"Execute update query on cassandra datasource."}
@Param {value:"query: Query to be executed"}
@Param {value:"parameters: Parameter array used with the given query"}
public native function <ClientConnector client> update (string queryString, (Parameter[]|null) parameters) returns
(CassandraConnectorError | null);

@Description {value:"The close action implementation to shutdown the cassandra connections."}
public native function <ClientConnector client> close () returns (CassandraConnectorError | null);


@Description {value:"CassandraConnectorError struct represents an error occured during the Cassandra client invocation"}
@Field {value:"message:  An error message explaining about the error"}
@Field {value:"cause: The error(s) that caused CassandraConnectorError to get thrown"}
public struct CassandraConnectorError {
    string message;
    error[] cause;
}

///////////////////////////////
// Cassandra Client Endpoint
/////////////////////////   //////

public struct Client {
    string epName;
    //ClientEndpointConfiguration config;
    ClientEndpointConfiguration clientEndpointConfig;
}

public struct ClientEndpointConfiguration {
    string host = "";
    int port = 0;
    string username = "";
    string password = "";
    ConnectionProperties options;
}

public function <ClientEndpointConfiguration c> ClientEndpointConfiguration () {
    c.options = {};
}

@Description {value:"Gets called when the endpoint is being initialize during package init time"}
@Param {value:"epName: The endpoint name"}
@Param {value:"config: The ClientEndpointConfiguration of the endpoint"}
public function <Client ep> init (ClientEndpointConfiguration config) {
    ep.clientEndpointConfig = config;
    ep.initEndpoint();
}

@Description {value:"Initialize the endpoint"}
public native function <Client ep> initEndpoint ();

@Description {value:"Returns the connector that client code uses"}
@Return {value:"The connector that client code uses"}
public native function <Client ep> getClient () returns (ClientConnector);