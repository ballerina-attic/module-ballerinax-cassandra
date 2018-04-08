/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.ballerinalang.cassandra.actions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;

/**
 * This class contains methods which are responsible for starting/cleaning up embedded cassandra server and creating
 * a session for assertion purposes.
 */
public class CassandraBaseTest {

    static Cluster cluster;
    static Session session;

    private static final int CASSANDRA_PORT = 9142;
    private static final String CASSANDRA_HOST = "localhost";

    @BeforeSuite(alwaysRun = true)
    public static void startAndSetupCassandraServer()
            throws InterruptedException, TTransportException, ConfigurationException, IOException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");

        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(CASSANDRA_HOST).build();
        cluster = builder.withPort(CASSANDRA_PORT).build();
        session = cluster.connect();
    }

    @AfterSuite(alwaysRun = true)
    public static void stopCassandraServer() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }
}
