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
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import org.ballerinalang.model.types.BType;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;

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

    public boolean init(String host, BStruct options) {
        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(host.split(",")).build();
        if (options != null) {
            builder = this.createOptions(builder, options);
        }
        this.cluster = builder.build();
        this.session = this.cluster.connect();
        return true;
    }

    private Cluster.Builder createOptions(Cluster.Builder builder, BStruct options) {
        boolean sslEnabled = options.getBooleanField(0) != 0;
        if (sslEnabled) {
            builder = builder.withSSL();
        }
        QueryOptions queryOpts = new QueryOptions();
        String consistencyLevel = options.getStringField(0);
        if (!consistencyLevel.isEmpty()) {
            queryOpts.setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel));
        }
        int fetchSize = (int) options.getIntField(0);
        if (fetchSize != -1) {
            queryOpts.setFetchSize(fetchSize);
        }
        return builder.withQueryOptions(queryOpts);
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
}
