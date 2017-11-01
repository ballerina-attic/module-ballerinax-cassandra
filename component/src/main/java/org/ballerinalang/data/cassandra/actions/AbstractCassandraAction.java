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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.data.cassandra.CassandraDataIterator;
import org.ballerinalang.data.cassandra.CassandraDataSource;
import org.ballerinalang.data.cassandra.Constants;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BDataTable;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code AbstractCassandraAction} is the base class for all Cassandra actions.
 *
 */
public abstract class AbstractCassandraAction extends AbstractNativeAction {

    public BDataTable execute(CassandraDataSource dataSource, String query, Object[] args) {
        ResultSet rs = dataSource.getSession().execute(query, args);
        return new BDataTable(new CassandraDataIterator(rs), this.extractColumnDefs(rs));
    }

    protected void close(CassandraDataSource dbDataSource) {
        dbDataSource.getSession().close();
        dbDataSource.getCluster().close();
    }

    protected ConnectorFuture getConnectorFuture() {
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

    protected CassandraDataSource getDataSource(BConnector bConnector) {
        CassandraDataSource datasource = null;
        BMap sharedMap = (BMap) bConnector.getRefField(1);
        if (sharedMap.get(new BString(Constants.DATASOURCE_KEY)) != null) {
            BValue value = sharedMap.get(new BString(Constants.DATASOURCE_KEY));
            if (value instanceof CassandraDataSource) {
                datasource = (CassandraDataSource) value;
            }
        } else {
            throw new BallerinaException("datasource not initialized properly");
        }
        return datasource;
    }

    private List<BDataTable.ColumnDefinition> extractColumnDefs(ResultSet rs) {
        List<BDataTable.ColumnDefinition> columnDefs = new ArrayList<BDataTable.ColumnDefinition>();
        for (ColumnDefinitions.Definition def : rs.getColumnDefinitions().asList()) {
            columnDefs.add(new BDataTable.ColumnDefinition(def.getName(), this.convert(def.getType()), 0));
        }
        return columnDefs;
    }

    private TypeKind convert(DataType type) {
        if (DataType.ascii().equals(type)) {
            return TypeKind.STRING;
        } else if (DataType.text().equals(type)) {
            return TypeKind.STRING;
        } else if (DataType.uuid().equals(type)) {
            return TypeKind.STRING;
        } else if (DataType.varchar().equals(type)) {
            return TypeKind.STRING;
        } else if (DataType.bigint().equals(type)) {
            return TypeKind.INT;
        } else if (DataType.cint().equals(type)) {
            return TypeKind.INT;
        } else if (DataType.counter().equals(type)) {
            return TypeKind.INT;
        } else if (DataType.date().equals(type)) {
            return TypeKind.INT;
        } else if (DataType.decimal().equals(type)) {
            return TypeKind.FLOAT;
        } else if (DataType.smallint().equals(type)) {
            return TypeKind.INT;
        } else if (DataType.time().equals(type)) {
            return TypeKind.INT;
        } else if (DataType.timestamp().equals(type)) {
            return TypeKind.INT;
        } else if (DataType.tinyint().equals(type)) {
            return TypeKind.INT;
        } else if (DataType.varint().equals(type)) {
            return TypeKind.INT;
        } else if (DataType.cboolean().equals(type)) {
            return TypeKind.BOOLEAN;
        } else if (DataType.cdouble().equals(type)) {
            return TypeKind.FLOAT;
        } else if (DataType.cfloat().equals(type)) {
            return TypeKind.FLOAT;
        } else if (DataType.blob().equals(type)) {
            return TypeKind.BLOB;
        }  else {
            return TypeKind.STRING;
        }
    }

}
