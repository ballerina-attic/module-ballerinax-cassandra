/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import org.ballerinalang.cassandra.BCursorTable;
import org.ballerinalang.cassandra.CassandraDataIterator;
import org.ballerinalang.cassandra.CassandraDataSource;
import org.ballerinalang.cassandra.CassandraDataSourceUtils;
import org.ballerinalang.cassandra.Constants;
import org.ballerinalang.jvm.ColumnDefinition;
import org.ballerinalang.jvm.StringUtils;
import org.ballerinalang.jvm.types.BArrayType;
import org.ballerinalang.jvm.types.BPackage;
import org.ballerinalang.jvm.types.BStructureType;
import org.ballerinalang.jvm.types.BTypes;
import org.ballerinalang.jvm.values.ArrayValue;
import org.ballerinalang.jvm.values.TypedescValue;
import org.ballerinalang.jvm.values.api.BArray;
import org.ballerinalang.jvm.values.api.BMap;
import org.ballerinalang.jvm.values.api.BRefValue;
import org.ballerinalang.jvm.values.api.BString;
import org.ballerinalang.jvm.values.api.BValueCreator;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.wso2.ballerinalang.compiler.util.TypeTags;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * {@code ActionUtil} is the base class for all Cassandra connector actions.
 *
 * @since 0.95.0
 */
class ActionUtil {

    static BCursorTable executeSelect(CassandraDataSource dataSource, String query,
                                ArrayValue parameters, TypedescValue recordType) {
        BArray uniformParams = constructUniformArrayOfParameters(parameters);
        String processedQuery = createProcessedQueryString(query, uniformParams);
        PreparedStatement preparedStatement = dataSource.getSession().prepare(processedQuery);
        BoundStatement stmt = createBoundStatement(preparedStatement, uniformParams);
        ResultSet rs = dataSource.getSession().execute(stmt);
        BStructureType structureType = recordType != null ? (BStructureType) recordType.getDescribingType() : null;
        return new BCursorTable(new CassandraDataIterator(rs, getColumnDefinitions(rs), structureType), structureType);
    }

    static void executeUpdate(CassandraDataSource dataSource, String query,
                              ArrayValue parameters) {
        BArray uniformParams = constructUniformArrayOfParameters(parameters);
        String processedQuery = createProcessedQueryString(query, uniformParams);
        PreparedStatement preparedStatement = dataSource.getSession().prepare(processedQuery);
        BoundStatement stmt = createBoundStatement(preparedStatement, uniformParams);
        dataSource.getSession().execute(stmt);
    }

    static void close(CassandraDataSource dbDataSource) {
        dbDataSource.getSession().close();
        dbDataSource.getCluster().close();
    }

    private static List<ColumnDefinition> getColumnDefinitions(ResultSet rs) {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        Set<String> columnNames = new HashSet<>();
        for (ColumnDefinitions.Definition def : rs.getColumnDefinitions().asList()) {
            String colName = def.getName();
            if (columnNames.contains(colName)) {
                String tableName = def.getTable().toUpperCase(Locale.ENGLISH);
                colName = tableName + "." + colName;
            }
            columnDefs.add(new ColumnDefinition(colName, convert(def.getType())));
            columnNames.add(colName);
        }
        return columnDefs;
    }

    private static int convert(DataType type) {
        if (DataType.ascii().equals(type)) {
            return TypeTags.STRING;
        } else if (DataType.text().equals(type)) {
            return TypeTags.STRING;
        } else if (DataType.uuid().equals(type)) {
            return TypeTags.STRING;
        } else if (DataType.varchar().equals(type)) {
            return TypeTags.STRING;
        } else if (DataType.bigint().equals(type)) {
            return TypeTags.INT;
        } else if (DataType.cint().equals(type)) {
            return TypeTags.INT;
        } else if (DataType.counter().equals(type)) {
            return TypeTags.INT;
        } else if (DataType.date().equals(type)) {
            return TypeTags.INT;
        } else if (DataType.decimal().equals(type)) {
            return TypeTags.FLOAT;
        } else if (DataType.smallint().equals(type)) {
            return TypeTags.INT;
        } else if (DataType.time().equals(type)) {
            return TypeTags.INT;
        } else if (DataType.timestamp().equals(type)) {
            return TypeTags.INT;
        } else if (DataType.tinyint().equals(type)) {
            return TypeTags.INT;
        } else if (DataType.varint().equals(type)) {
            return TypeTags.INT;
        } else if (DataType.cboolean().equals(type)) {
            return TypeTags.BOOLEAN;
        } else if (DataType.cdouble().equals(type)) {
            return TypeTags.FLOAT;
        } else if (DataType.cfloat().equals(type)) {
            return TypeTags.FLOAT;
        } else if (DataType.blob().equals(type)) {
            return TypeTags.ARRAY;
        } else {
            return TypeTags.STRING;
        }
    }

    /**
     * Search for the first occurrence of "?" from the given starting point and replace it with given number of "?"'s.
     */
    private static Object[] expandQuery(int start, int count, String query) {
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
                result.append(generateQuestionMarks(count));
                end = result.length() + 1;
                if (i + 1 < n) {
                    result.append(query.substring(i + 1));
                }
                break;
            }
        }
        return new Object[]{end, result.toString()};
    }

    private static String generateQuestionMarks(int n) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < n; i++) {
            builder.append(Constants.QUESTION_MARK);
            if (i + 1 < n) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    private static String getCQLType(BMap parameter) {
        return (String) parameter.get(Constants.CQL_TYPE_FIELD);
    }

    private static void bindValue(ArrayList<Object> dataList, Object value, String csqlType) {
        String cSQLDataType = csqlType.toUpperCase(Locale.getDefault());
        switch (cSQLDataType) {
            case Constants.DataTypes.INT:
                dataList.add(Integer.parseInt(String.valueOf(value)));
                break;
            case Constants.DataTypes.BIGINT:
                dataList.add(Long.parseLong(String.valueOf(value)));
                break;
            case Constants.DataTypes.VARINT:
            case Constants.DataTypes.FLOAT:
                dataList.add(Float.parseFloat(String.valueOf(value)));
                break;
            case Constants.DataTypes.DOUBLE:
                dataList.add(Double.parseDouble(String.valueOf(value)));
                break;
            case Constants.DataTypes.TEXT:
                dataList.add(String.valueOf(value));
                break;
            case Constants.DataTypes.BOOLEAN:
                dataList.add(Boolean.parseBoolean(String.valueOf(value)));
                break;
        }
    }

    private static BoundStatement createBoundStatement(PreparedStatement stmt, BArray params) {
        ArrayList<Object> dataList = new ArrayList<>();
        BoundStatement boundStmt = stmt.bind();
        if (params == null) {
            return boundStmt;
        }
        int paramCount = (int) params.size();
        for (int index = 0; index < paramCount; index++) {
            BMap<String, Object> paramStruct = (BMap<String, Object>) params.get(index);
            if (paramStruct != null) {
                String cqlType = getCQLType(paramStruct);
                Object value = paramStruct.get(Constants.VALUE_FIELD);
                //If the parameter is an array and sql type is not "array" then treat it as an array of parameters
                if (value instanceof BArray && !Constants.DataTypes.LIST
                        .equalsIgnoreCase(cqlType)) {
                    int arrayLength = (int) ((BArray) value).size();
                    int typeTag = ((BArray) ((BArray) value).getType()).getElementType().getTag();
                    for (int i = 0; i < arrayLength; i++) {
                        Object paramValue;
                        switch (typeTag) {
                            case TypeTags.INT:
                                paramValue = ((BArray) value).getInt(i);
                                break;
                            case TypeTags.FLOAT:
                                paramValue = ((BArray) value).getFloat(i);
                                break;
                            case TypeTags.STRING:
                                paramValue = ((BArray) value).getBString(i);
                                break;
                            case TypeTags.BOOLEAN:
                                paramValue = ((BArray) value).getBoolean(i);
                                break;
                            case TypeTags.ARRAY:
                                Object array = ((BArray) value).get(i);
                                if (((BArrayType) ((BArray) value).getType()).getElementType().getTag() ==
                                        TypeTags.BYTE) {
                                    paramValue = array;
                                    break;
                                } else {
                                    throw new BallerinaException("unsupported array type for parameter index: " +
                                                                         index + ". Array element type being an array" +
                                                                         " is supported only when the inner array" +
                                                                         " element type is BYTE");
                                }
                            default:
                                throw new BallerinaException("unsupported array type for parameter index " + index);
                        }
                        bindValue(dataList, paramValue, cqlType);
                    }
                } else {
                    bindValue(dataList, value, cqlType);
                }
            } else {
                dataList.add(null);
            }
        }
        boundStmt.bind(dataList.toArray());
        return boundStmt;
    }

    private static BArray constructUniformArrayOfParameters(ArrayValue inputParams) {
        int count = inputParams.size();
        BArrayType arrayType = new BArrayType(BTypes.typeMap);
        BArray uniformParams = BValueCreator.createArrayValue(arrayType);
        for (int i = 0; i < count; i++) {
            BRefValue typeValue = (BRefValue) inputParams.getRefValue(i);
            BMap<BString, Object> param;
            if (typeValue.getType().getTag() == TypeTags.RECORD) {
                param = (BMap<BString, Object>) typeValue;
            } else {
                param = BValueCreator.createRecordValue(new BPackage("ballerina", "cassandra"),
                                                        Constants.CASSANDRA_PARAMETER);
                param.put(StringUtils.fromString(Constants.CQL_TYPE_FIELD),
                          CassandraDataSourceUtils.getCQLType(typeValue.getType()));
                param.put(StringUtils.fromString(Constants.VALUE_FIELD), typeValue);
            }
            uniformParams.add(i, param);
        }
        return uniformParams;
    }

    /**
     * If there are any arrays of parameter for types other than sql array, the given query is expanded by adding "?" s
     * to match with the array size.
     */
    private static String createProcessedQueryString(String query, BArray parameters) {
        String currentQuery = query;
        if (parameters != null) {
            int start = 0;
            Object[] vals;
            int count;
            int paramCount = (int) parameters.size();
            for (int i = 0; i < paramCount; i++) {
                BMap param = (BMap) parameters.get(i);
                if (param != null) {
                    String cqlType = getCQLType(param);
                    Object value = param.get(Constants.VALUE_FIELD);
                    if (value instanceof BArray && !Constants.DataTypes.LIST.equalsIgnoreCase(cqlType)) {
                        count = ((BArray) value).size();
                    } else {
                        count = 1;
                    }
                    vals = expandQuery(start, count, currentQuery);
                    start = (Integer) vals[0];
                    currentQuery = (String) vals[1];
                }
            }
        }
        return currentQuery;
    }
}
