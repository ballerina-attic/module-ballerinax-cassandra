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

package org.ballerinalang.data.cassandra.endpoint;

import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BlockingNativeCallableUnit;
import org.ballerinalang.connector.api.BLangConnectorSPIUtil;
import org.ballerinalang.connector.api.Struct;
import org.ballerinalang.data.cassandra.CassandraDataSource;
import org.ballerinalang.data.cassandra.Constants;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

/**
 * Initiates the data source.
 *
 * @since 0.5.4
 */

@BallerinaFunction(
        orgName = "ballerina", packageName = "data.cassandra",
        functionName = "initEndpoint",
        receiver = @Receiver(type = TypeKind.STRUCT, structType = "Client",
                             structPackage = "ballerina.data.cassandra"),
        args = {@Argument(name = "epName", type = TypeKind.STRING),
                @Argument(name = "config", type = TypeKind.STRUCT, structType = "ClientEndpointConfiguration")},
        isPublic = true
)
public class InitEndpoint extends BlockingNativeCallableUnit {

    @Override
    public void execute(Context context) {
        Struct clientEndpoint = BLangConnectorSPIUtil.getConnectorEndpointStruct(context);
        Struct clientEndpointConfig = clientEndpoint.getStructField(Constants.CLIENT_ENDPOINT_CONFIG);

        //Extract parameters from the endpoint config
        String host = clientEndpointConfig.getStringField(Constants.EndpointConfig.HOST);
        int port = (int) clientEndpointConfig.getIntField(Constants.EndpointConfig.PORT);
        String username = clientEndpointConfig.getStringField(Constants.EndpointConfig.USERNAME);
        String password = clientEndpointConfig.getStringField(Constants.EndpointConfig.PASSWORD);
        Struct options = clientEndpointConfig.getStructField(Constants.EndpointConfig.OPTIONS);

        CassandraDataSource dataSource = new CassandraDataSource();
        dataSource.init(host, port, username, password, options);

        BStruct ballerinaClientConnector;
        if (clientEndpoint.getNativeData(Constants.B_CONNECTOR) != null) {
            ballerinaClientConnector = (BStruct) clientEndpoint.getNativeData(Constants.B_CONNECTOR);
        } else {
            ballerinaClientConnector = BLangConnectorSPIUtil
                    .createBStruct(context.getProgramFile(), Constants.CASSANDRA_PACKAGE_PATH,
                            Constants.CLIENT_CONNECTOR, host, port, username, password, options, clientEndpointConfig);
            clientEndpoint.addNativeData(Constants.B_CONNECTOR, ballerinaClientConnector);
        }

        ballerinaClientConnector.addNativeData(Constants.CLIENT_CONNECTOR, dataSource);
        context.setReturnValues();
    }
}
