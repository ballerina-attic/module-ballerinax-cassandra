import ballerina.data.cassandra as c;

const int port = 9142;
const string host = "localhost";
const string username = "cassandra";
const string password = "cassandra";

function testConnectionInitWithLBPolicy() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {loadBalancingPolicy: "DCAwareRoundRobinPolicy"});
    }
    conn.update("CREATE KEYSPACE lbtestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor' :1}", null);
    conn.close();
}

function testConnectionInitWithInvalidLBPolicy() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {loadBalancingPolicy: "InvalidRoundRobinPolicy"});
    }
}

function testConnectionInitWithRetryPolicy() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {retryPolicy:"DefaultRetryPolicy"});
    }
    conn.update("CREATE KEYSPACE retrytestkeyspace  WITH replication = {'class': 'SimpleStrategy', 'replication_factor':
    1}", null);
    conn.close();
}

function testConnectionInitWithInvalidRetryPolicy() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {retryPolicy: "InvalidRetryPolicy"});
    }
    conn.close();
}

function testConnectionInitWithReconnectionPolicy() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {
         reconnectionPolicy:"ConstantReconnectionPolicy", constantReconnectionPolicyDelay:500});
    }
    conn.update("CREATE KEYSPACE reconnectiontestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor': 1}", null);
    conn.close();
}

function testConnectionInitWithInvalidReconnectionPolicy() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {
         reconnectionPolicy: "InvalidReconnectionPolicy", constantReconnectionPolicyDelay: 500});
    }
}

function testConnectionInitWithInvalidConsistencyLevel() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {queryOptionsConfig: {
         consistencyLevel: "INVALID_LEVEL"}});
    }
}

function testConnectionInitWithInvalidSerialConsistencyLevel() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {queryOptionsConfig: {
         serialConsistencyLevel: "ONE"}});
    }
}


function testConnectionInitWithPoolingOptions() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {poolingOptionsConfig: {
         maxRequestsPerConnectionLocal: 1, maxRequestsPerConnectionRemote: 128,    idleTimeoutSeconds: 120,
         poolTimeoutMillis: 100,    maxQueueSize: 256,    heartbeatIntervalSeconds: 30,
         coreConnectionsPerHostLocal: 2, maxConnectionsPerHostLocal: 2, newConnectionThresholdLocal: 100,
         coreConnectionsPerHostRemote: 1, maxConnectionsPerHostRemote: 8, newConnectionThresholdRemote: 100}});
    }
    conn.update("CREATE KEYSPACE poolingoptionstestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor': 1}", null);
    conn.close();
}


function testConnectionInitWithSocketOptions() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {socketOptionsConfig: {
         connectTimeoutMillis: 5000, readTimeoutMillis: 12000, soLinger: 0}
         });
    }
    conn.update("CREATE KEYSPACE socketoptionstestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor': 1}", null);
    conn.close();
}

function testConnectionInitWithQueryOptions() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {queryOptionsConfig: {
         consistencyLevel: "ONE", serialConsistencyLevel: "LOCAL_SERIAL", defaultIdempotence: false, metadataEnabled:
          true, reprepareOnUp: true, prepareOnAllHosts:true, fetchSize: 5000, maxPendingRefreshNodeListRequests: 20,
         maxPendingRefreshNodeRequests: 20, maxPendingRefreshSchemaRequests: 20, refreshNodeListIntervalMillis: 1000,
         refreshNodeIntervalMillis: 1000, refreshSchemaIntervalMillis: 1000}});
    }
    conn.update("CREATE KEYSPACE queryoptionstestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor': 1}", null);
    conn.close();
}

function testConnectionInitWithProtocolOptions() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {protocolOptionsConfig: {
         sslEnabled: false, noCompact: false, maxSchemaAgreementWaitSeconds: 10, compression: "NONE",
         initialProtocolVersion:"V4"}});
    }
    conn.update("CREATE KEYSPACE protocoloptionstestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor' :1}", null);
    conn.close();
}

function testConnectionInitWithAdditionalConnectionParams() {
    endpoint<c:ClientConnector> conn {
         create c:ClientConnector(host, port, username, password, {clusterName: "Maze", withoutMetrics:
         false, withoutJMXReporting: false, allowRemoteDCsForLocalConsistencyLevel: false});
    }
    conn.update("CREATE KEYSPACE conparamtestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor': 1}", null);
    conn.close();
}






