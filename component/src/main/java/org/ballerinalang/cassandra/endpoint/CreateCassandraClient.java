/*
 * Copyright (c) 2018, WSO2 Inc. (http:www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http:www.apache.orglicensesLICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.ballerinalang.cassandra.endpoint;

import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BlockingNativeCallableUnit;
import org.ballerinalang.cassandra.CassandraDataSource;
import org.ballerinalang.cassandra.Constants;
import org.ballerinalang.connector.api.BLangConnectorSPIUtil;
import org.ballerinalang.connector.api.Struct;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;

/**
 * Creates a Cassandra client.
 *
 * @since 0.5.4
 */
@BallerinaFunction(
        orgName = "wso2",
        packageName = "cassandra:0.0.0",
        functionName = "createClient",
        args = {
                @Argument(name = "clientEndpointConfig",
                          type = TypeKind.RECORD,
                          structType = "ClientEndpointConfiguration")
        },
        isPublic = true
)
public class CreateCassandraClient extends BlockingNativeCallableUnit {

    @Override
    public void execute(Context context) {
        BMap<String, BValue> configBStruct = (BMap<String, BValue>) context.getRefArgument(0);
        Struct clientEndpointConfig = BLangConnectorSPIUtil.toStruct(configBStruct);

        //Extract parameters from the endpoint config
        String host = clientEndpointConfig.getStringField(Constants.EndpointConfig.HOST);
        int port = (int) clientEndpointConfig.getIntField(Constants.EndpointConfig.PORT);
        String username = clientEndpointConfig.getStringField(Constants.EndpointConfig.USERNAME);
        String password = clientEndpointConfig.getStringField(Constants.EndpointConfig.PASSWORD);
        Struct options = clientEndpointConfig.getStructField(Constants.EndpointConfig.OPTIONS);

        CassandraDataSource dataSource = new CassandraDataSource();
        dataSource.init(host, port, username, password, options);

        BMap<String, BValue> cassandraClient = BLangConnectorSPIUtil
                .createBStruct(context.getProgramFile(), Constants.CASSANDRA_PACKAGE_PATH, Constants.CALLER_ACTIONS);
        cassandraClient.addNativeData(Constants.CALLER_ACTIONS, dataSource);
        context.setReturnValues(cassandraClient);
    }
}
