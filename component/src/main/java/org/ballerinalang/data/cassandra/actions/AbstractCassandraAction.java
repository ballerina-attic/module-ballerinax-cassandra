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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.data.cassandra.CassandraDataIterator;
import org.ballerinalang.data.cassandra.CassandraDataSource;
import org.ballerinalang.data.cassandra.Constants;
import org.ballerinalang.model.ColumnDefinition;
import org.ballerinalang.model.types.BArrayType;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.types.TypeTags;
import org.ballerinalang.model.values.BBlob;
import org.ballerinalang.model.values.BBlobArray;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BBooleanArray;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BDataTable;
import org.ballerinalang.model.values.BFloat;
import org.ballerinalang.model.values.BFloatArray;
import org.ballerinalang.model.values.BIntArray;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BNewArray;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStringArray;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.exceptions.ArgumentOutOfRangeException;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * {@code AbstractCassandraAction} is the base class for all Cassandra connector actions.
 *
 * @since 0.95.0
 */
public abstract class AbstractCassandraAction extends AbstractNativeAction {

    @Override
    public BValue getRefArgument(Context context, int index) {
        if (index > -1) {
            return context.getControlStackNew().getCurrentFrame().getRefLocalVars()[index];
        }
        throw new ArgumentOutOfRangeException(index);
    }

    public BDataTable executeSelect(CassandraDataSource dataSource, String query, BRefValueArray parameters) {
        String processedQuery = createProcessedQueryString(query, parameters);
        PreparedStatement preparedStatement = dataSource.getSession().prepare(processedQuery);
        BoundStatement stmt = createBoundStatement(preparedStatement, parameters);
        ResultSet rs = dataSource.getSession().execute(stmt);
        return new BDataTable(new CassandraDataIterator(rs, this.getColumnDefinitions(rs)));
    }

    public void executeUpdate(CassandraDataSource dataSource, String query, BRefValueArray parameters) {
        String processedQuery = createProcessedQueryString(query, parameters);
        PreparedStatement preparedStatement = dataSource.getSession().prepare(processedQuery);
        BoundStatement stmt = createBoundStatement(preparedStatement, parameters);
        dataSource.getSession().execute(stmt);
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

    private List<ColumnDefinition> getColumnDefinitions(ResultSet rs) {
        List<ColumnDefinition> columnDefs = new ArrayList<ColumnDefinition>();
        for (ColumnDefinitions.Definition def : rs.getColumnDefinitions().asList()) {
            columnDefs.add(new ColumnDefinition(def.getName(), this.convert(def.getType())));
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

    /**
     * If there are any arrays of parameter for types other than sql array, the given query is expanded by adding "?" s
     * to match with the array size.
     */
    public String createProcessedQueryString(String query, BRefValueArray parameters) {
        String currentQuery = query;
        if (parameters != null) {
            int start = 0;
            Object[] vals;
            int count;
            int paramCount = (int) parameters.size();
            for (int i = 0; i < paramCount; i++) {
                BStruct paramValue = (BStruct) parameters.get(i);
                if (paramValue != null) {
                    BValue value = paramValue.getRefField(0);
                    String sqlType = paramValue.getStringField(0);
                    if (value != null && value.getType().getTag() == TypeTags.ARRAY_TAG
                            && !org.ballerinalang.nativeimpl.actions.data.sql.Constants.SQLDataTypes.ARRAY
                            .equalsIgnoreCase(sqlType)) {
                        count = (int) ((BNewArray) value).size();
                    } else {
                        count = 1;
                    }
                    vals = this.expandQuery(start, count, currentQuery);
                    start = (Integer) vals[0];
                    currentQuery = (String) vals[1];
                }
            }
        }
        return currentQuery;
    }

    /**
     * Search for the first occurrence of "?" from the given starting point and replace it with given number of "?"'s.
     */
    private Object[] expandQuery(int start, int count, String query) {
        StringBuilder result = new StringBuilder();
        int n = query.length();
        boolean doubleQuoteExists = false;
        boolean singleQuoteExists = false;
        int end = n;
        for (int i = start; i < n; i++) {
            if (query.charAt(i) == '\'') {
                singleQuoteExists = !singleQuoteExists;
            } else if (query.charAt(i) == '\"') {
                doubleQuoteExists = !doubleQuoteExists;
            } else if (query.charAt(i) == '?' && !(doubleQuoteExists || singleQuoteExists)) {
                result.append(query.substring(0, i));
                result.append(this.generateQuestionMarks(count));
                end = result.length() + 1;
                if (i + 1 < n) {
                    result.append(query.substring(i + 1));
                }
                break;
            }
        }
        return new Object[] { end, result.toString() };
    }

    private String generateQuestionMarks(int n) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < n; i++) {
            builder.append(Constants.QUESTION_MARK);
            if (i + 1 < n) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    private BoundStatement createBoundStatement(PreparedStatement stmt, BRefValueArray params) {
        ArrayList<Object> dataList = new ArrayList<Object>();
        BoundStatement boundStmt = stmt.bind();
        if (params == null) {
            return boundStmt;
        }
        int paramCount = (int) params.size();
        for (int index = 0; index < paramCount; index++) {
            BStruct paramStruct = (BStruct) params.get(index);
            if (paramStruct != null) {
                String cqlType = paramStruct.getStringField(0);
                BValue value = paramStruct.getRefField(0);
                //If the parameter is an array and sql type is not "array" then treat it as an array of parameters
                if (value != null && value.getType().getTag() == TypeTags.ARRAY_TAG && !Constants.DataTypes.LIST
                        .equalsIgnoreCase(cqlType)) {
                    int arrayLength = (int) ((BNewArray) value).size();
                    int typeTag = ((BArrayType) value.getType()).getElementType().getTag();
                    for (int i = 0; i < arrayLength; i++) {
                        Object paramValue;
                        switch (typeTag) {
                        case TypeTags.INT_TAG:
                            paramValue = new BInteger(((BIntArray) value).get(i));
                            break;
                        case TypeTags.FLOAT_TAG:
                            paramValue = new BFloat(((BFloatArray) value).get(i));
                            break;
                        case TypeTags.STRING_TAG:
                            paramValue = new BString(((BStringArray) value).get(i));
                            break;
                        case TypeTags.BOOLEAN_TAG:
                            paramValue = new BBoolean(((BBooleanArray) value).get(i) > 0);
                            break;
                        case TypeTags.BLOB_TAG:
                            paramValue = new BBlob(((BBlobArray) value).get(i));
                            break;
                        default:
                            throw new BallerinaException("unsupported array type for parameter index " + index);
                        }
                        dataList.add(paramValue);
                        //boundStmt.bind(paramValue);
                    }
                } else {
                    bindValue(dataList, value, cqlType);
                }
            } else {
                dataList.add(null);
                //boundStmt.bind(null);
            }
        }
        boundStmt.bind(dataList.toArray());
        return boundStmt;
    }

    private void bindValue(ArrayList<Object> dataList, BValue value, String csqlType) {
        String cSQLDataType = csqlType.toUpperCase(Locale.getDefault());
        if (Constants.DataTypes.INT.equals(cSQLDataType)) {
            dataList.add(Integer.parseInt(value.stringValue()));
        } else if (Constants.DataTypes.BIGINT.equals(cSQLDataType)) {
            dataList.add(Long.parseLong(value.stringValue()));
        } else if (Constants.DataTypes.VARINT.equals(cSQLDataType)) {
            dataList.add(Float.parseFloat(value.stringValue()));
        } else if (Constants.DataTypes.FLOAT.equals(cSQLDataType)) {
            dataList.add(Float.parseFloat(value.stringValue()));
        } else if (Constants.DataTypes.DOUBLE.equals(cSQLDataType)) {
            dataList.add(Double.parseDouble(value.stringValue()));
        } else if (Constants.DataTypes.TEXT.equals(cSQLDataType)) {
            dataList.add(value.stringValue());
        } else if (Constants.DataTypes.BOOLEAN.equals(cSQLDataType)) {
            dataList.add(Boolean.parseBoolean(value.stringValue()));
        }
    }

}
