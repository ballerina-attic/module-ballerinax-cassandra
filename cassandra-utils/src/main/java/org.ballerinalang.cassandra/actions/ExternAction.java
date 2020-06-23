/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.cassandra.actions;

import org.ballerinalang.cassandra.CassandraDataSource;
import org.ballerinalang.cassandra.Constants;
import org.ballerinalang.jvm.BallerinaErrors;
import org.ballerinalang.jvm.StringUtils;
import org.ballerinalang.jvm.util.exceptions.BallerinaConnectorException;
import org.ballerinalang.jvm.values.ArrayValue;
import org.ballerinalang.jvm.values.MapValue;
import org.ballerinalang.jvm.values.ObjectValue;
import org.ballerinalang.jvm.values.TypedescValue;
import org.ballerinalang.jvm.values.api.BString;

/**
 * Util class for Cassandra client action handling.
 *
 * @since 0.8.5
 */
public class ExternAction {

    public static void init(ObjectValue cassandraClient, MapValue<BString, Object> clientConfig) {
        String host = clientConfig.getStringValue(Constants.EndpointConfig.HOST).getValue();
        int port = Math.toIntExact(clientConfig.getIntValue(Constants.EndpointConfig.PORT));
        String username = clientConfig.getStringValue(Constants.EndpointConfig.USERNAME).getValue();
        String password = clientConfig.getStringValue(Constants.EndpointConfig.PASSWORD).getValue();
        MapValue options = clientConfig.getMapValue(Constants.EndpointConfig.OPTIONS);
        CassandraDataSource dataSource = new CassandraDataSource();
        dataSource.init(host, port, username, password, options);
        cassandraClient.addNativeData(Constants.CLIENT, dataSource);
    }

    public static void close(ObjectValue cassandraClient) {
        CassandraDataSource dataSource = (CassandraDataSource) cassandraClient.getNativeData(Constants.CLIENT);
        try {
            ActionUtil.close(dataSource);
        } catch (Throwable e) {
            throw new BallerinaConnectorException("error occurred while closing the client: ", e);
        }
    }

    public static Object selectData(ObjectValue cassandraClient, BString queryString, TypedescValue recordType,
                                ArrayValue parameters) {
        CassandraDataSource dataSource = (CassandraDataSource) cassandraClient.getNativeData(Constants.CLIENT);
        try {
            return ActionUtil.executeSelect(dataSource, queryString.getValue(), parameters, recordType);
        } catch (Throwable e) {
            return BallerinaErrors.createError(
                    StringUtils.fromString("Error occurred while executing the select statement: "
                            + e.getMessage()));
        }
    }

    public static Object update(ObjectValue cassandraClient, BString queryString, ArrayValue parameters) {
        CassandraDataSource dataSource = (CassandraDataSource) cassandraClient.getNativeData(Constants.CLIENT);
        try {
            ActionUtil.executeUpdate(dataSource, queryString.getValue(), parameters);
            return null;
        } catch (Throwable e) {
            return BallerinaErrors.createError(Constants.DATABASE_ERROR_CODE, "Error occurred while executing the " +
                    "update statement: " + e.getMessage());
        }
    }

    private ExternAction() {
    }
}
