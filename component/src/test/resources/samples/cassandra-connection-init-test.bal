import ballerina/data.cassandra as c;

const int port = 9142;
const string host = "localhost";
const string username = "cassandra";
const string password = "cassandra";

function testConnectionInitWithLBPolicy() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{loadBalancingPolicy:"DCAwareRoundRobinPolicy"}
    };
    _ = conn -> update("CREATE KEYSPACE lbtestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor' :1}", null);
    _ = conn -> close();
}

function testConnectionInitWithInvalidLBPolicy() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{loadBalancingPolicy:"InvalidRoundRobinPolicy"}
    };
}

function testConnectionInitWithRetryPolicy() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{retryPolicy:"DefaultRetryPolicy"}
    };

    _ = conn -> update("CREATE KEYSPACE retrytestkeyspace  WITH replication = {'class': 'SimpleStrategy', 'replication_factor':
    1}", null);
    _ = conn -> close();
}

function testConnectionInitWithInvalidRetryPolicy() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{retryPolicy:"InvalidRetryPolicy"}
    };
    _ = conn -> close();
}

function testConnectionInitWithReconnectionPolicy() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{reconnectionPolicy:"ConstantReconnectionPolicy", constantReconnectionPolicyDelay:500}
    };

    _ = conn -> update("CREATE KEYSPACE reconnectiontestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor': 1}", null);
    _ = conn -> close();
}

function testConnectionInitWithInvalidReconnectionPolicy() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{reconnectionPolicy:"InvalidReconnectionPolicy", constantReconnectionPolicyDelay:500}
    };
}

function testConnectionInitWithInvalidConsistencyLevel() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{queryOptionsConfig: {
                                         consistencyLevel: "INVALID_LEVEL"}}
    };

}

function testConnectionInitWithInvalidSerialConsistencyLevel() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{queryOptionsConfig: {
                                         serialConsistencyLevel: "ONE"}}
    };
}


function testConnectionInitWithPoolingOptions() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{poolingOptionsConfig:{
                                           maxRequestsPerConnectionLocal:1, maxRequestsPerConnectionRemote:128, idleTimeoutSeconds:120,
                                           poolTimeoutMillis:100, maxQueueSize:256, heartbeatIntervalSeconds:30,
                                           coreConnectionsPerHostLocal:2, maxConnectionsPerHostLocal:2, newConnectionThresholdLocal:100,
                                           coreConnectionsPerHostRemote:1, maxConnectionsPerHostRemote:8,
                                          newConnectionThresholdRemote:100}}};
    _ = conn -> update("CREATE KEYSPACE poolingoptionstestkeyspace  WITH replication = {'class': 'SimpleStrategy',
        'replication_factor': 1}", null);
    _ = conn -> close();

}


function testConnectionInitWithSocketOptions() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{socketOptionsConfig: {
                                          connectTimeoutMillis: 5000, readTimeoutMillis: 12000, soLinger: 0}}
    };
    _ = conn -> update("CREATE KEYSPACE socketoptionstestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor': 1}", null);
    _ = conn -> close();
}

function testConnectionInitWithQueryOptions() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{queryOptionsConfig:{
                                        consistencyLevel:"ONE", serialConsistencyLevel:"LOCAL_SERIAL", defaultIdempotence:false, metadataEnabled:
                                                                                                                                 true, reprepareOnUp:true, prepareOnAllHosts:true, fetchSize:5000, maxPendingRefreshNodeListRequests:20,
                                        maxPendingRefreshNodeRequests:20, maxPendingRefreshSchemaRequests:20, refreshNodeListIntervalMillis:1000,
                                        refreshNodeIntervalMillis:1000, refreshSchemaIntervalMillis:1000}}
    };
    _ = conn -> update("CREATE KEYSPACE queryoptionstestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor': 1}", null);
    _ = conn -> close();
}

function testConnectionInitWithProtocolOptions() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{protocolOptionsConfig: {
                                            sslEnabled: false, noCompact: false, maxSchemaAgreementWaitSeconds: 10, compression: "NONE",
                                            initialProtocolVersion:"V4"}}
    };

    _ = conn -> update("CREATE KEYSPACE protocoloptionstestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor' :1}", null);
    _ = conn -> close();
}

function testConnectionInitWithAdditionalConnectionParams() {
    endpoint c:Client conn {
        host:"localhost",
        port:9142,
        username:"cassandra",
        password:"cassandra",
        options:{clusterName: "Maze", withoutMetrics:
                                      false, withoutJMXReporting: false, allowRemoteDCsForLocalConsistencyLevel: false}
    };

    _ = conn -> update("CREATE KEYSPACE conparamtestkeyspace  WITH replication = {'class': 'SimpleStrategy',
    'replication_factor': 1}", null);
    _ = conn -> close();
}






