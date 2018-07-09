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
package org.ballerinalang.cassandra.actions;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BlockingNativeCallableUnit;
import org.ballerinalang.cassandra.CassandraDataIterator;
import org.ballerinalang.cassandra.CassandraDataSource;
import org.ballerinalang.cassandra.CassandraDataSourceUtils;
import org.ballerinalang.cassandra.Constants;
import org.ballerinalang.database.table.BCursorTable;
import org.ballerinalang.model.ColumnDefinition;
import org.ballerinalang.model.types.BArrayType;
import org.ballerinalang.model.types.BStructureType;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.types.TypeTags;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BBooleanArray;
import org.ballerinalang.model.values.BFloat;
import org.ballerinalang.model.values.BFloatArray;
import org.ballerinalang.model.values.BIntArray;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BNewArray;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStringArray;
import org.ballerinalang.model.values.BTable;
import org.ballerinalang.model.values.BTypeDescValue;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.codegen.PackageInfo;
import org.ballerinalang.util.codegen.StructureTypeInfo;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * {@code AbstractCassandraAction} is the base class for all Cassandra connector actions.
 *
 * @since 0.95.0
 */
public abstract class AbstractCassandraAction extends BlockingNativeCallableUnit {

    public BTable executeSelect(Context context, CassandraDataSource dataSource, String query,
            BRefValueArray parameters, BStructureType type) {
        BRefValueArray uniformParams = constructUniformArrayOfParameters(parameters, context);
        String processedQuery = createProcessedQueryString(query, uniformParams);
        PreparedStatement preparedStatement = dataSource.getSession().prepare(processedQuery);
        BoundStatement stmt = createBoundStatement(preparedStatement, uniformParams);
        ResultSet rs = dataSource.getSession().execute(stmt);
        return new BCursorTable(new CassandraDataIterator(rs, this.getColumnDefinitions(rs), type), false);
    }

    public void executeUpdate(Context context, CassandraDataSource dataSource, String query,
            BRefValueArray parameters) {
        BRefValueArray uniformParams = constructUniformArrayOfParameters(parameters, context);
        String processedQuery = createProcessedQueryString(query, uniformParams);
        PreparedStatement preparedStatement = dataSource.getSession().prepare(processedQuery);
        BoundStatement stmt = createBoundStatement(preparedStatement, uniformParams);
        dataSource.getSession().execute(stmt);
    }

    protected void close(CassandraDataSource dbDataSource) {
        dbDataSource.getSession().close();
        dbDataSource.getCluster().close();
    }

    protected BStructureType getStructType(Context context) {
        BStructureType structType = null;
        BTypeDescValue type = (BTypeDescValue) context.getNullableRefArgument(1);
        if (type != null) {
            structType = (BStructureType) type.value();
        }
        return structType;
    }

    private BRefValueArray constructUniformArrayOfParameters(BRefValueArray inputParams, Context context) {
        BRefValueArray uniformParams = new BRefValueArray();
        int count = (int) inputParams.size();
        for (int i = 0; i < count; i++) {
            BRefType typeValue = inputParams.get(i);
            BMap<String, BValue> param;
            if (typeValue.getType().getTag() == TypeTags.RECORD_TYPE_TAG) {
                param = (BMap<String, BValue>) typeValue;
            } else {
                param = createCQLParameter(context);
                param.put(Constants.CQL_TYPE_FIELD,
                        new BString(CassandraDataSourceUtils.getCQLType(typeValue.getType())));
                param.put(Constants.VALUE_FIELD, typeValue);
            }
            uniformParams.add(i, param);
        }
        return uniformParams;
    }

    private static BMap<String, BValue> createCQLParameter(Context context) {
        PackageInfo sqlPackageInfo = context.getProgramFile().getPackageInfo(Constants.CASSANDRA_PACKAGE_PATH);
        StructureTypeInfo paramStructInfo = sqlPackageInfo.getStructInfo(Constants.CASSANDRA_PARAMETER);
        return new BMap<>(paramStructInfo.getType());
    }

    private List<ColumnDefinition> getColumnDefinitions(ResultSet rs) {
        List<ColumnDefinition> columnDefs = new ArrayList<ColumnDefinition>();
        Set<String> columnNames = new HashSet<>();
        for (ColumnDefinitions.Definition def : rs.getColumnDefinitions().asList()) {
            String colName = def.getName();
            if (columnNames.contains(colName)) {
                String tableName = def.getTable().toUpperCase(Locale.ENGLISH);
                colName = tableName + "." + colName;
            }
            columnDefs.add(new ColumnDefinition(colName, this.convert(def.getType())));
            columnNames.add(colName);
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
            return TypeKind.ARRAY;
        }  else {
            return TypeKind.STRING;
        }
    }

    /**
     * If there are any arrays of parameter for types other than sql array, the given query is expanded by adding "?" s
     * to match with the array size.
     */
    private String createProcessedQueryString(String query, BRefValueArray parameters) {
        String currentQuery = query;
        if (parameters != null) {
            int start = 0;
            Object[] vals;
            int count;
            int paramCount = (int) parameters.size();
            for (int i = 0; i < paramCount; i++) {
                BMap<String, BValue> paramStruct = (BMap<String, BValue>) parameters.get(i);
                if (paramStruct != null) {
                    String cqlType = getCQLType(paramStruct);
                    BValue value = paramStruct.get(Constants.VALUE_FIELD);
                    if (value != null && value.getType().getTag() == TypeTags.ARRAY_TAG && !Constants.DataTypes.LIST
                            .equalsIgnoreCase(cqlType)) {
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
        ArrayList<Object> dataList = new ArrayList<>();
        BoundStatement boundStmt = stmt.bind();
        if (params == null) {
            return boundStmt;
        }
        int paramCount = (int) params.size();
        for (int index = 0; index < paramCount; index++) {
            BMap<String, BValue> paramStruct = (BMap<String, BValue>) params.get(index);
            if (paramStruct != null) {
                String cqlType = getCQLType(paramStruct);
                BValue value = paramStruct.get(Constants.VALUE_FIELD);
                //If the parameter is an array and sql type is not "array" then treat it as an array of parameters
                if (value != null && value.getType().getTag() == TypeTags.ARRAY_TAG && !Constants.DataTypes.LIST
                        .equalsIgnoreCase(cqlType)) {
                    int arrayLength = (int) ((BNewArray) value).size();
                    int typeTag = ((BArrayType) value.getType()).getElementType().getTag();
                    for (int i = 0; i < arrayLength; i++) {
                        BValue paramValue;
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
                        case TypeTags.ARRAY_TAG:
                            BValue array = ((BRefValueArray) value).get(i);
                            if (((BArrayType) value.getType()).getElementType().getTag() == TypeTags.BYTE_TAG) {
                                paramValue = array;
                                break;
                            } else {
                                throw new BallerinaException("unsupported array type for parameter index: " + index
                                        + ". Array element type being an array is supported only when the inner array"
                                        + " element type is BYTE");
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

    private String getCQLType(BMap<String, BValue> parameter) {
        String cqlType = "";
        BValue refType = parameter.get(Constants.CQL_TYPE_FIELD);
        if (refType != null) {
            cqlType = refType.stringValue();
        }
        return cqlType;
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
