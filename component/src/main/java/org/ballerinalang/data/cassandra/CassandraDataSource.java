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
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.exceptions.BallerinaException;

/**
 * {@code CassandraDataSource} util class for Cassandra connector initialization.
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

    public boolean init(String host, BMap mapProperties) {
        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(host.split(",")).build();
        if (mapProperties != null && !mapProperties.isEmpty()) {
            builder = this.createOptions(builder, mapProperties);
        }
        this.cluster = builder.build();
        this.session = this.cluster.connect();
        return true;
    }

    private Cluster.Builder createOptions(Cluster.Builder builder, BMap options) {
        BValue value = options.get(new BString(Constants.SSL_ENABLED));
        if (value != null && Boolean.parseBoolean(value.stringValue())) {
            builder = builder.withSSL();
        }
        QueryOptions queryOpts = new QueryOptions();
        value = options.get(new BString(Constants.CONSISTENCY_LEVEL));
        if (value != null) {
            queryOpts.setConsistencyLevel(ConsistencyLevel.valueOf(value.stringValue()));
        }
        value = options.get(new BString(Constants.FETCH_SIZE));
        if (value != null) {
            try {
                queryOpts.setFetchSize(Integer.parseInt(value.stringValue()));
            } catch (NumberFormatException e) {
                throw new BallerinaException("fetch size must be an integer value: " + value.stringValue(), e);
            }
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
