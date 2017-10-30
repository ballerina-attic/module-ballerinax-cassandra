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
package org.ballerinalang.data.cassandra.actions;

import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.data.cassandra.CassandraDataSource;
import org.ballerinalang.data.cassandra.Constants;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BDataTable;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.natives.annotations.ReturnType;
/**
 * {@code Execute} action executes a given query.
 */
@BallerinaAction(
        packageName = "ballerina.data.cassandra",
        actionName = "execute",
        connectorName = Constants.CONNECTOR_NAME,
        args = {@Argument(name = "c", type = TypeKind.CONNECTOR),
                @Argument(name = "query", type = TypeKind.STRING)
        },
        returnType = { @ReturnType(type = TypeKind.DATATABLE) }
)
public class Execute extends AbstractCassandraAction {

    @Override
    public ConnectorFuture execute(Context context) {
        BConnector bConnector = (BConnector) getRefArgument(context, 0);
        String query = getStringArgument(context, 0);
        CassandraDataSource dataSource = getDataSource(bConnector);
        BDataTable dataTable = execute(dataSource, query, null); //TODO:add args
        context.getControlStackNew().getCurrentFrame().returnValues[0] = dataTable;
        return getConnectorFuture();
    }
}
