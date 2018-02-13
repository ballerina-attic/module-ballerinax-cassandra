/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.ballerinalang.data.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.ballerinalang.model.types.BType;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.HashMap;
import java.util.Map;

/**
 * {@code CassandraDataSource} util class for Cassandra connector initialization.
 *
 * @since 0.95.0
 */
public class CassandraDataSource implements BValue {

    private Cluster cluster;

    private Session session;

    public Cluster getCluster() {
        return cluster;
    }

    public Session getSession() {
        return session;
    }

    /**
     * Initializes the Cassandra cluster.
     *
     * @param host     Host(s) Cassandra instance(s) reside(s)
     * @param port     Port for the Cassandra instance(s)
     * @param username Username if authentication is enabled
     * @param password Password if authentication is enabled
     * @param options  BStruct containing available options for cluster connection initialization
     * @return true if initialization is successful
     */
    public boolean init(String host, int port, String username, String password, BStruct options) {
        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(host.split(",")).build();
        if (port != -1) {
            builder.withPort(port);
        }
        populateAuthenticationOptions(builder, username, password);
        if (options != null) {
            builder = this.populateOptions(builder, options);
        }
        this.cluster = builder.build();
        this.session = this.cluster.connect();
        return true;
    }

    /**
     * Populates the builder with Cassandra cluster initialization options.
     *
     * @param builder Cluster Builder
     * @param options BStruct containing available options for cluster connection initialization
     * @return Populated Cluster Builder
     */
    private Cluster.Builder populateOptions(Cluster.Builder builder, BStruct options) {
        BStruct queryOptionsConfig = (BStruct) options.getRefField(ConnectionParam.QUERY_OPTIONS.getIndex());
        if (queryOptionsConfig != null) {
            populateQueryOptions(builder, queryOptionsConfig);
        }
        BStruct poolingOptionsConfig = (BStruct) options.getRefField(ConnectionParam.POOLING_OPTIONS.getIndex());
        if (poolingOptionsConfig != null) {
            populatePoolingOptions(builder, poolingOptionsConfig);
        }
        BStruct socketOptionsConfig = (BStruct) options.getRefField(ConnectionParam.SOCKET_OPTIONS.getIndex());
        if (socketOptionsConfig != null) {
            populateSocketOptions(builder, socketOptionsConfig);
        }
        BStruct protocolOptionsConfig = (BStruct) options.getRefField(ConnectionParam.PROTOCOL_OPTIONS.getIndex());
        if (protocolOptionsConfig != null) {
            populateProtocolOptions(builder, protocolOptionsConfig);
        }
        String clusterName = options.getStringField(ConnectionParam.CLUSTER_NAME.getIndex());
        if (!clusterName.isEmpty()) {
            builder.withClusterName(clusterName);
        }
        boolean jmxReportingDisabled = options.getBooleanField(ConnectionParam.WITHOUT_JMX_REPORTING.getIndex()) != 0;
        if (jmxReportingDisabled) {
            builder.withoutJMXReporting();
        }
        boolean metricsDisabled = options.getBooleanField(ConnectionParam.WITHOUT_METRICS.getIndex()) != 0;
        if (metricsDisabled) {
            builder.withoutMetrics();
        }
        populateLoadBalancingPolicy(builder, options);
        populateReconnectionPolicy(builder, options);
        populateRetryPolicy(builder, options);

        return builder;
    }

    /**
     * Populates the Cassnadra RetryPolicy options in the Cluster Builder.
     *
     * @param builder Cluster Builder
     * @param options BStruct containing available options for cluster connection initialization
     */
    private void populateRetryPolicy(Cluster.Builder builder, BStruct options) {
        String retryPolicyString = options.getStringField(ConnectionParam.RETRY_POLICY.getIndex());
        if (!retryPolicyString.isEmpty()) {
            RetryPolicy retryPolicy = retrieveRetryPolicy(retryPolicyString);
            switch (retryPolicy) {
            case DEFAULT_RETRY_POLICY:
                builder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);
                break;
            case FALLTHROUGH_RETRY_POLICY:
                builder.withRetryPolicy(FallthroughRetryPolicy.INSTANCE);
                break;
            case DOWNGRADING_CONSISTENCY_RETRY_POLICY:
                builder.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
                break;
            case LOGGING_DEFAULT_RETRY_POLICY:
                builder.withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE));
                break;
            case LOGGING_FALLTHROUGH_RETRY_POLICY:
                builder.withRetryPolicy(new LoggingRetryPolicy(FallthroughRetryPolicy.INSTANCE));
                break;
            case LOGGING_DOWNGRADING_CONSISTENCY_RETRY_POLICY:
                builder.withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE));
                break;
            default:
                throw new UnsupportedOperationException(
                        "Support for the retry policy \"" + retryPolicy + "\" is not implemented yet");
            }
        }
    }

    private RetryPolicy retrieveRetryPolicy(String retryPolicy) {
        try {
            return RetryPolicy.fromPolicyName(retryPolicy);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("\"" + retryPolicy + "\" is not a valid retry policy");
        }
    }

    /**
     * Populates Reconnection Policy options in the Cluster Builder.
     *
     * @param builder Cluster Builder
     * @param options BStruct containing available options for cluster connection initialization
     */
    private void populateReconnectionPolicy(Cluster.Builder builder, BStruct options) {
        String reconnectionPolicyString = options.getStringField(ConnectionParam.RECONNECTION_POLICY.getIndex());

        if (!reconnectionPolicyString.isEmpty()) {
            ReconnectionPolicy reconnectionPolicy = retrieveReconnectionPolicy(reconnectionPolicyString);
            switch (reconnectionPolicy) {
            case CONSTANT_RECONNECTION_POLICY:
                long constantReconnectionPolicyDelay = options
                        .getIntField(ConnectionParam.CONSTANT_RECONNECTION_POLICY_DELAY.getIndex());
                if (constantReconnectionPolicyDelay != -1) {
                    builder.withReconnectionPolicy(new ConstantReconnectionPolicy(constantReconnectionPolicyDelay));
                } else {
                    throw new BallerinaException("constantReconnectionPolicyDelay required for the "
                            + "initialization of ConstantReconnectionPolicy, has not been set");
                }
                break;
            case EXPONENTIAL_RECONNECTION_POLICY:
                long exponentialReconnectionPolicyBaseDelay = options
                        .getIntField(ConnectionParam.EXPONENTIAL_RECONNECTION_POLICY_BASE_DELAY.getIndex());
                long exponentialReconnectionPolicyMaxDelay = options
                        .getIntField(ConnectionParam.EXPONENTIAL_RECONNECTION_POLICY_MAX_DELAY.getIndex());
                if (exponentialReconnectionPolicyBaseDelay != -1 && exponentialReconnectionPolicyMaxDelay != -1) {
                    builder.withReconnectionPolicy(
                            new ExponentialReconnectionPolicy(exponentialReconnectionPolicyBaseDelay,
                                    exponentialReconnectionPolicyMaxDelay));
                } else {
                    throw new BallerinaException("exponentialReconnectionPolicyBaseDelay or "
                            + "exponentialReconnectionPolicyMaxDelay required for the "
                            + "initialization of ConstantReconnectionPolicy, has not been set");
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Support for the reconnection policy \"" + reconnectionPolicy + "\" is not implemented yet");
            }
        }

    }

    private ReconnectionPolicy retrieveReconnectionPolicy(String reconnectionPolicy) {
        try {
            return ReconnectionPolicy.fromPolicyName(reconnectionPolicy);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("\"" + reconnectionPolicy + "\"" + " is not a valid reconnection policy");
        }
    }

    /**
     * Populates LoadBalancing Policy options in the Cluster Builder.
     *
     * @param builder Cluster Builder
     * @param options BStruct containing available options for cluster connection initialization
     */
    private void populateLoadBalancingPolicy(Cluster.Builder builder, BStruct options) {
        String dataCenter = options.getStringField(ConnectionParam.DATA_CENTER.getIndex());
        String loadBalancingPolicyString = options.getStringField(ConnectionParam.LOAD_BALANCING_POLICY.getIndex());
        boolean allowRemoteDCsForLocalConsistencyLevel =
                options.getBooleanField(ConnectionParam.ALLOW_REMOTE_DCS_FOR_LOCAL_CONSISTENCY_LEVEL.getIndex()) != 0;

        if (!loadBalancingPolicyString.isEmpty()) {
            LoadBalancingPolicy loadBalancingPolicy = retrieveLoadBalancingPolicy(loadBalancingPolicyString);
            switch (loadBalancingPolicy) {
            case DC_AWARE_ROUND_ROBIN_POLICY:
                if (dataCenter != null && !dataCenter.isEmpty()) {
                    if (allowRemoteDCsForLocalConsistencyLevel) {
                        builder.withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(dataCenter)
                                .allowRemoteDCsForLocalConsistencyLevel().build());
                    } else {
                        builder.withLoadBalancingPolicy(
                                DCAwareRoundRobinPolicy.builder().withLocalDc(dataCenter).build());
                    }
                } else {
                    if (allowRemoteDCsForLocalConsistencyLevel) {
                        builder.withLoadBalancingPolicy(
                                (DCAwareRoundRobinPolicy.builder().allowRemoteDCsForLocalConsistencyLevel().build()));
                    } else {
                        builder.withLoadBalancingPolicy((DCAwareRoundRobinPolicy.builder().build()));
                    }
                }
                break;
            case LATENCY_AWARE_ROUND_ROBIN_POLICY:
                builder.withLoadBalancingPolicy(LatencyAwarePolicy.builder(new RoundRobinPolicy()).build());
                break;
            case ROUND_ROBIN_POLICY:
                builder.withLoadBalancingPolicy(new RoundRobinPolicy());
                break;
            case TOKEN_AWARE_ROUND_ROBIN_POLICY:
                builder.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
                break;
            default:
                throw new UnsupportedOperationException(
                        "Support for the load balancing policy \"" + loadBalancingPolicy + "\" is not implemented yet");
            }
        }
    }

    private LoadBalancingPolicy retrieveLoadBalancingPolicy(String loadBalancingPolicy) {
        try {
            return LoadBalancingPolicy.fromPolicyName(loadBalancingPolicy);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("\"" + loadBalancingPolicy + "\"" + " is not a valid load balancing policy");
        }
    }

    /**
     * Populates Authentication Options in the Cluster Builder.
     *
     * @param builder  Cluster Builder
     * @param username Provided Username
     * @param password Provided Password
     */
    private void populateAuthenticationOptions(Cluster.Builder builder, String username, String password) {
        // TODO: Enable functionality for custom authenticators
        builder.withAuthProvider(new PlainTextAuthProvider(username, password));
    }

    /**
     * Populates Query Options in the Cluster Builder.
     *
     * @param builder            Cluster Builder
     * @param queryOptionsConfig BStruct containing available query options for cluster connection initialization
     */
    private void populateQueryOptions(Cluster.Builder builder, BStruct queryOptionsConfig) {
        QueryOptions queryOptions = new QueryOptions();

        String consistencyLevel = queryOptionsConfig.getStringField(QueryOptionsParam.CONSISTENCY_LEVEL.getIndex());
        String serialConsistencyLevel = queryOptionsConfig
                .getStringField(QueryOptionsParam.SERIAL_CONSISTENCY_LEVEL.getIndex());
        boolean defaultIdempotence =
                queryOptionsConfig.getBooleanField(QueryOptionsParam.DEFAULT_IDEMPOTENCE.getIndex()) != 0;
        boolean metadataEnabled =
                queryOptionsConfig.getBooleanField(QueryOptionsParam.METADATA_ENABLED.getIndex()) != 0;
        boolean reprepareOnUp = queryOptionsConfig.getBooleanField(QueryOptionsParam.REPREPARE_ON_UP.getIndex()) != 0;
        queryOptions.setReprepareOnUp(reprepareOnUp);
        boolean prepareOnAllHosts =
                queryOptionsConfig.getBooleanField(QueryOptionsParam.PREPARE_ON_ALL_HOSTS.getIndex()) != 0;
        queryOptions.setPrepareOnAllHosts(prepareOnAllHosts);
        int fetchSize = (int) queryOptionsConfig.getIntField(QueryOptionsParam.FETCH_SIZE.getIndex());
        int maxPendingRefreshNodeListRequests = (int) queryOptionsConfig
                .getIntField(QueryOptionsParam.MAX_PENDING_REFRESH_NODELIST_REQUESTS.getIndex());
        int maxPendingRefreshNodeRequests = (int) queryOptionsConfig
                .getIntField(QueryOptionsParam.MAX_PENDING_REFRESH_NODE_REQUESTS.getIndex());
        int maxPendingRefreshSchemaRequests = (int) queryOptionsConfig
                .getIntField(QueryOptionsParam.MAX_PENDING_REFRESH_SCHEMA_REQUESTS.getIndex());
        int refreshNodeListIntervalMillis = (int) queryOptionsConfig
                .getIntField(QueryOptionsParam.REFRESH_NODELIST_INTERVAL_MILLIS.getIndex());
        int refreshNodeIntervalMillis = (int) queryOptionsConfig
                .getIntField(QueryOptionsParam.REFRESH_NODE_INTERNAL_MILLIS.getIndex());
        int refreshSchemaIntervalMillis = (int) queryOptionsConfig
                .getIntField(QueryOptionsParam.REFRESH_SCHEMA_INTERVAL_MILLIS.getIndex());

        if (!consistencyLevel.isEmpty()) {
            queryOptions.setConsistencyLevel(retrieveConsistencyLevel(consistencyLevel));
        }
        if (!serialConsistencyLevel.isEmpty()) {
            queryOptions.setSerialConsistencyLevel(retrieveSerialConsistencyLevel(serialConsistencyLevel));
        }
        queryOptions.setDefaultIdempotence(defaultIdempotence);
        queryOptions.setMetadataEnabled(metadataEnabled);
        if (fetchSize != -1) {
            queryOptions.setFetchSize(fetchSize);
        }
        if (maxPendingRefreshNodeListRequests != -1) {
            queryOptions.setMaxPendingRefreshNodeListRequests(maxPendingRefreshNodeListRequests);
        }
        if (maxPendingRefreshNodeRequests != -1) {
            queryOptions.setMaxPendingRefreshNodeRequests(maxPendingRefreshNodeRequests);
        }
        if (maxPendingRefreshSchemaRequests != -1) {
            queryOptions.setMaxPendingRefreshSchemaRequests(maxPendingRefreshSchemaRequests);
        }
        if (refreshNodeListIntervalMillis != -1) {
            queryOptions.setRefreshNodeIntervalMillis(refreshNodeListIntervalMillis);
        }
        if (refreshNodeIntervalMillis != -1) {
            queryOptions.setRefreshNodeIntervalMillis(refreshNodeIntervalMillis);
        }
        if (refreshSchemaIntervalMillis != -1) {
            queryOptions.setRefreshSchemaIntervalMillis(refreshSchemaIntervalMillis);
        }
        builder.withQueryOptions(queryOptions);
    }

    /**
     * Populates Pooling Options in the Cluster Builder.
     *
     * @param builder              Cluster Builder
     * @param poolingOptionsConfig BStruct containing available pooling options for cluster connection initialization
     */
    private void populatePoolingOptions(Cluster.Builder builder, BStruct poolingOptionsConfig) {
        PoolingOptions poolingOptions = new PoolingOptions();

        int coreConnectionsPerHostLocal = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.CORE_CONNECTIONS_PER_HOST_LOCAL.getIndex());
        int maxConnectionsPerHostLocal = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.MAX_CONNECTIONS_PER_HOST_LOCAL.getIndex());
        int newConnectionThresholdLocal = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.NEW_CONNECTION_THRESHOLD_LOCAL.getIndex());
        int coreConnectionsPerHostRemote = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.CORE_CONNECTIONS_PER_HOST_REMOTE.getIndex());
        int maxConnectionsPerHostRemote = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.MAX_CONNECTIONS_PER_HOST_REMOTE.getIndex());
        int newConnectionThresholdRemote = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.NEW_CONNECTION_THRESHOLD_REMOTE.getIndex());
        int maxRequestsPerConnectionLocal = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.MAX_REQUESTS_PER_CONNECTION_LOCAL.getIndex());
        int maxRequestsPerConnectionRemote = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.MAX_REQUESTS_PER_CONNECTION_REMOTE.getIndex());
        int idleTimeoutSeconds = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.IDLE_TIMEOUT_SECONDS.getIndex());
        int poolTimeoutMillis = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.POOL_TIMEOUT_MILLIS.getIndex());
        int maxQueueSize = (int) poolingOptionsConfig.getIntField(PoolingOptionsParam.MAX_QUEUE_SIZE.getIndex());
        int heartbeatIntervalSeconds = (int) poolingOptionsConfig
                .getIntField(PoolingOptionsParam.HEART_BEAT_INTERVAL_SECONDS.getIndex());

        if (coreConnectionsPerHostLocal != -1) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnectionsPerHostLocal);
        }
        if (coreConnectionsPerHostRemote != -1) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, coreConnectionsPerHostRemote);
        }
        if (maxConnectionsPerHostLocal != -1) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsPerHostLocal);
        }
        if (maxConnectionsPerHostRemote != -1) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnectionsPerHostRemote);
        }
        if (newConnectionThresholdLocal != -1) {
            poolingOptions.setNewConnectionThreshold(HostDistance.LOCAL, newConnectionThresholdLocal);
        }
        if (newConnectionThresholdRemote != -1) {
            poolingOptions.setNewConnectionThreshold(HostDistance.REMOTE, newConnectionThresholdRemote);
        }
        if (maxRequestsPerConnectionLocal != -1) {
            poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, maxRequestsPerConnectionLocal);
        }
        if (maxRequestsPerConnectionRemote != -1) {
            poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE, maxRequestsPerConnectionRemote);
        }
        if (idleTimeoutSeconds != -1) {
            poolingOptions.setIdleTimeoutSeconds(idleTimeoutSeconds);
        }
        if (poolTimeoutMillis != -1) {
            poolingOptions.setPoolTimeoutMillis(poolTimeoutMillis);
        }
        if (maxQueueSize != -1) {
            poolingOptions.setMaxQueueSize(maxQueueSize);
        }
        if (heartbeatIntervalSeconds != -1) {
            poolingOptions.setHeartbeatIntervalSeconds(heartbeatIntervalSeconds);
        }

        builder.withPoolingOptions(poolingOptions);
    }

    /**
     * Populates Socket Options in the Cluster Builder.
     *
     * @param builder             Cluster Builder
     * @param socketOptionsConfig BStruct containing available socket options for cluster connection initialization
     */
    private void populateSocketOptions(Cluster.Builder builder, BStruct socketOptionsConfig) {
        SocketOptions socketOptions = new SocketOptions();

        int connectTimeoutMillis = (int) socketOptionsConfig
                .getIntField(SocketOptionsParam.CONNECT_TIMEOUT_MILLIS.getIndex());
        int readTimeoutMillis = (int) socketOptionsConfig
                .getIntField(SocketOptionsParam.READ_TIMEOUT_MILLIS.getIndex());
        int soLinger = (int) socketOptionsConfig.getIntField(SocketOptionsParam.SO_LINGER.getIndex());
        int receiveBufferSize = (int) socketOptionsConfig
                .getIntField(SocketOptionsParam.RECEIVE_BUFFER_SIZE.getIndex());
        int sendBufferSize = (int) socketOptionsConfig.getIntField(SocketOptionsParam.SEND_BUFFER_SIZE.getIndex());

        if (connectTimeoutMillis != -1) {
            socketOptions.setConnectTimeoutMillis(connectTimeoutMillis);
        }
        if (readTimeoutMillis != -1) {
            socketOptions.setReadTimeoutMillis(readTimeoutMillis);
        }
        if (soLinger != -1) {
            socketOptions.setSoLinger(soLinger);
        }
        if (receiveBufferSize != -1) {
            socketOptions.setReceiveBufferSize(receiveBufferSize);
        }
        if (sendBufferSize != -1) {
            socketOptions.setSendBufferSize(sendBufferSize);
        }

    /* TODO: Driver does not set these by default. It takes the default values of the underlying netty transport.
        But if we take the inputs as booleans and if the value set in the bal file is "false", we wouldn't know
        if it was set by the user or not. So need to decide how to handle this.
        boolean keepAlive = socketOptionsConfig.getBooleanField(SocketOptionsParam.KEEP_ALIVE.getIndex()) != 0;
        boolean reuseAddress = socketOptionsConfig.getBooleanField(SocketOptionsParam.REUSE_ADDRESS.getIndex()) != 0;
        boolean tcpNoDelay = socketOptionsConfig.getBooleanField(SocketOptionsParam.TCP_NO_DELAY.getIndex()) != 0;

        socketOptions.setKeepAlive(keepAlive);
        socketOptions.setReuseAddress(reuseAddress);
        socketOptions.setTcpNoDelay(tcpNoDelay);
    */
        builder.withSocketOptions(socketOptions);
    }

    /**
     * Populates Protocol Options in the Cluster Builder.
     *
     * @param builder               Cluster Builder
     * @param protocolOptionsConfig BStruct containing available protocol options for cluster connection initialization
     */
    private void populateProtocolOptions(Cluster.Builder builder, BStruct protocolOptionsConfig) {
        boolean sslEnabled = protocolOptionsConfig.getBooleanField(ProtocolOptionsParam.SSL_ENABLED.getIndex()) != 0;
        boolean noCompact = protocolOptionsConfig.getBooleanField(ProtocolOptionsParam.NO_COMPACT.getIndex()) != 0;

        int maxSchemaAgreementWaitSeconds = (int) protocolOptionsConfig
                .getIntField(ProtocolOptionsParam.MAX_SCHEMA_AGREEMENT_WAIT_SECONDS.getIndex());
        String compression = protocolOptionsConfig.getStringField(ProtocolOptionsParam.COMPRESSION.getIndex());
        String initialProtocolVersion = protocolOptionsConfig
                .getStringField(ProtocolOptionsParam.INITIAL_PROTOCOL_VERSION.getIndex());

        if (sslEnabled) {
            builder = builder.withSSL();
        }
        if (noCompact) {
            builder.withNoCompact();
        }
        if (maxSchemaAgreementWaitSeconds != -1) {
            builder.withMaxSchemaAgreementWaitSeconds(maxSchemaAgreementWaitSeconds);
        }
        if (!compression.isEmpty()) {
            builder.withCompression(retrieveCompression(compression));
        }
        if (!initialProtocolVersion.isEmpty()) {
            builder.withProtocolVersion(retrieveProtocolVersion(initialProtocolVersion));
        }
    }

    @Override
    public String stringValue() {
        return null;
    }

    @Override
    public BType getType() {
        return null;
    }

    @Override
    public BValue copy() {
        return null;
    }

    private ProtocolVersion retrieveProtocolVersion(String protocolVersion) {
        try {
            return ProtocolVersion.valueOf(protocolVersion);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("\"" + protocolVersion + "\" is not a valid protocol version");
        }
    }

    private ProtocolOptions.Compression retrieveCompression(String compression) {
        try {
            return ProtocolOptions.Compression.valueOf(compression);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("\"" + compression + "\" is not a valid compression type");
        }
    }

    private ConsistencyLevel retrieveSerialConsistencyLevel(String consistencyLevel) {
        try {
            ConsistencyLevel serialConsistencyLevel = ConsistencyLevel.valueOf(consistencyLevel);
            if (serialConsistencyLevel.isSerial()) {
                return serialConsistencyLevel;
            } else {
                throw new BallerinaException("\"" + consistencyLevel + "\" is not a valid serial consistency level");
            }
        } catch (DriverInternalError e) {
            throw new BallerinaException("\"" + consistencyLevel + "\" is not a valid serial consistency level");
        }
    }

    private ConsistencyLevel retrieveConsistencyLevel(String consistencyLevel) {
        try {
            return ConsistencyLevel.valueOf(consistencyLevel);
        } catch (DriverInternalError e) {
            throw new BallerinaException("\"" + consistencyLevel + "\" is not a valid consistency level");
        }
    }

    private enum SocketOptionsParam {
        // int params
        CONNECT_TIMEOUT_MILLIS(0), READ_TIMEOUT_MILLIS(1), SO_LINGER(2), RECEIVE_BUFFER_SIZE(3), SEND_BUFFER_SIZE(4),

        // boolean params
        KEEP_ALIVE(0), REUSE_ADDRESS(1), TCP_NO_DELAY(2);

        private int index;

        SocketOptionsParam(int index) {
            this.index = index;
        }

        private int getIndex() {
            return index;
        }
    }

    private enum PoolingOptionsParam {
        // int params
        MAX_REQUESTS_PER_CONNECTION_LOCAL(0), MAX_REQUESTS_PER_CONNECTION_REMOTE(1), IDLE_TIMEOUT_SECONDS(
                2), POOL_TIMEOUT_MILLIS(3), MAX_QUEUE_SIZE(4), HEART_BEAT_INTERVAL_SECONDS(
                5), CORE_CONNECTIONS_PER_HOST_LOCAL(6), MAX_CONNECTIONS_PER_HOST_LOCAL(
                7), NEW_CONNECTION_THRESHOLD_LOCAL(8), CORE_CONNECTIONS_PER_HOST_REMOTE(
                9), MAX_CONNECTIONS_PER_HOST_REMOTE(10), NEW_CONNECTION_THRESHOLD_REMOTE(11);

        private int index;

        PoolingOptionsParam(int index) {
            this.index = index;
        }

        private int getIndex() {
            return index;
        }
    }

    private enum QueryOptionsParam {
        // string params
        CONSISTENCY_LEVEL(0), SERIAL_CONSISTENCY_LEVEL(1),

        // boolean params
        DEFAULT_IDEMPOTENCE(0), METADATA_ENABLED(1), REPREPARE_ON_UP(2), PREPARE_ON_ALL_HOSTS(3),

        // int params
        FETCH_SIZE(0), MAX_PENDING_REFRESH_NODELIST_REQUESTS(1), MAX_PENDING_REFRESH_NODE_REQUESTS(
                2), MAX_PENDING_REFRESH_SCHEMA_REQUESTS(3), REFRESH_NODELIST_INTERVAL_MILLIS(
                4), REFRESH_NODE_INTERNAL_MILLIS(5), REFRESH_SCHEMA_INTERVAL_MILLIS(6);

        private int index;

        QueryOptionsParam(int index) {
            this.index = index;
        }

        private int getIndex() {
            return index;
        }
    }

    private enum ProtocolOptionsParam {
        // boolean params
        SSL_ENABLED(0), NO_COMPACT(1),

        // int params
        MAX_SCHEMA_AGREEMENT_WAIT_SECONDS(0),

        // string params
        INITIAL_PROTOCOL_VERSION(0), COMPRESSION(1);

        private int index;

        ProtocolOptionsParam(int index) {
            this.index = index;
        }

        private int getIndex() {
            return index;
        }
    }

    private enum ConnectionParam {
        // string params
        CLUSTER_NAME(0), LOAD_BALANCING_POLICY(1), RECONNECTION_POLICY(2), RETRY_POLICY(3), DATA_CENTER(4),

        // boolean params
        WITHOUT_METRICS(0), WITHOUT_JMX_REPORTING(1), ALLOW_REMOTE_DCS_FOR_LOCAL_CONSISTENCY_LEVEL(2),

        // int params
        CONSTANT_RECONNECTION_POLICY_DELAY(0), EXPONENTIAL_RECONNECTION_POLICY_BASE_DELAY(
                1), EXPONENTIAL_RECONNECTION_POLICY_MAX_DELAY(2),

        // ref params
        QUERY_OPTIONS(0), POOLING_OPTIONS(1), SOCKET_OPTIONS(2), PROTOCOL_OPTIONS(3);

        private int index;

        ConnectionParam(int index) {
            this.index = index;
        }

        private int getIndex() {
            return index;
        }

    }

    private enum LoadBalancingPolicy {
        DC_AWARE_ROUND_ROBIN_POLICY("DCAwareRoundRobinPolicy"), LATENCY_AWARE_ROUND_ROBIN_POLICY(
                "LatencyAwarePolicy"), ROUND_ROBIN_POLICY("RoundRobinPolicy"), TOKEN_AWARE_ROUND_ROBIN_POLICY(
                "TokenAwarePolicy");
        // TODO: Add support for HostFilterPolicy.
        // TODO: Add support for ErrorAwarePolicy which is still(datastax driver 3.4.0) in beta mode

        private String loadBalancingPolicy;

        private static final Map<String, LoadBalancingPolicy> policyMap = new HashMap<>();

        LoadBalancingPolicy(String loadBalancingPolicy) {
            this.loadBalancingPolicy = loadBalancingPolicy;
        }

        static {
            LoadBalancingPolicy[] policies = values();
            for (LoadBalancingPolicy policy : policies) {
                policyMap.put(policy.getPolicyName(), policy);
            }
        }

        public static LoadBalancingPolicy fromPolicyName(String policyName) {
            LoadBalancingPolicy policy = policyMap.get(policyName);
            if (policy == null) {
                throw new IllegalArgumentException("Unsupported Load Balancing policy: " + policyName);
            } else {
                return policy;
            }
        }

        private String getPolicyName() {
            return this.loadBalancingPolicy;
        }

    }

    private enum ReconnectionPolicy {
        CONSTANT_RECONNECTION_POLICY("ConstantReconnectionPolicy"), EXPONENTIAL_RECONNECTION_POLICY(
                "ExponentialReconnectionPolicy");

        private String reconnectionPolicy;

        private static final Map<String, ReconnectionPolicy> policyMap = new HashMap<>();

        ReconnectionPolicy(String reconnectionPolicy) {
            this.reconnectionPolicy = reconnectionPolicy;
        }

        static {
            ReconnectionPolicy[] policies = values();
            for (ReconnectionPolicy policy : policies) {
                policyMap.put(policy.getPolicyName(), policy);
            }
        }

        public static ReconnectionPolicy fromPolicyName(String policyName) {
            ReconnectionPolicy policy = policyMap.get(policyName);
            if (policy == null) {
                throw new IllegalArgumentException("Unsupported Reconnection policy: " + policyName);
            } else {
                return policy;
            }
        }

        private String getPolicyName() {
            return this.reconnectionPolicy;
        }

    }

    private enum RetryPolicy {
        DEFAULT_RETRY_POLICY("DefaultRetryPolicy"), DOWNGRADING_CONSISTENCY_RETRY_POLICY(
                "DowngradingConsistencyRetryPolicy"), FALLTHROUGH_RETRY_POLICY(
                "FallthroughRetryPolicy"), LOGGING_DEFAULT_RETRY_POLICY(
                "LoggingDefaultRetryPolicy"), LOGGING_DOWNGRADING_CONSISTENCY_RETRY_POLICY(
                "LoggingDowngradingConsistencyRetryPolicy"), LOGGING_FALLTHROUGH_RETRY_POLICY(
                "LoggingFallthroughRetryPolicy");

        private String retryPolicy;

        private static final Map<String, RetryPolicy> policyMap = new HashMap<>();

        RetryPolicy(String retryPolicy) {
            this.retryPolicy = retryPolicy;
        }

        private String getPolicyName() {
            return this.retryPolicy;
        }

        static {
            RetryPolicy[] policies = values();
            for (RetryPolicy policy : policies) {
                policyMap.put(policy.getPolicyName(), policy);
            }
        }

        public static RetryPolicy fromPolicyName(String policyName) {
            RetryPolicy mechanism = policyMap.get(policyName);
            if (mechanism == null) {
                throw new IllegalArgumentException("Unsupported Retry policy: " + policyName);
            } else {
                return mechanism;
            }
        }
    }
}
