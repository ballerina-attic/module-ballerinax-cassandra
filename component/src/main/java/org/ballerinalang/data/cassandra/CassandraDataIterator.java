/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.ballerinalang.data.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.ballerinalang.model.ColumnDefinition;
import org.ballerinalang.model.DataIterator;
import org.ballerinalang.model.types.BStructType;
import org.ballerinalang.model.types.BType;
import org.ballerinalang.model.types.BTypes;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.types.TypeTags;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This iterator wraps a cassandra data row.
 *
 * @since 0.95.0
 */
public class CassandraDataIterator implements DataIterator {

    private Iterator<Row> iterator;

    private Row current;

    private List<ColumnDefinition> columnDefs;

    private BStructType bStructType;

    public CassandraDataIterator(ResultSet rs, List<ColumnDefinition> columnDefs) {
        this.iterator = rs.iterator();
        this.columnDefs = columnDefs;
        generateStructType();
    }

    @Override
    public boolean next() {
        boolean result = this.iterator.hasNext();
        if (result) {
            this.current = this.iterator.next();
        }
        return result;
    }

    @Override
    public void close(boolean b) {
        /* ignore */
    }

    @Override
    public String getString(String s) {
        this.checkCurrentRow();
        return this.current.getString(s);
    }

    @Override
    public long getInt(String s) {
        this.checkCurrentRow();
        return this.current.getInt(s);
    }

    @Override
    public double getFloat(String s) {
        this.checkCurrentRow();
        return this.current.getFloat(s);
    }

    @Override
    public boolean getBoolean(String s) {
        this.checkCurrentRow();
        return this.current.getBool(s);
    }

    @Override
    public String getBlob(String s) {
        this.checkCurrentRow();
        return getString(this.current.getBytes(s));
    }

    @Override
    public Map<String, Object> getArray(String s) {
        return null;
    }

    @Override
    public BStruct generateNext() {
        BStruct bStruct = new BStruct(bStructType);
        int longRegIndex = -1;
        int doubleRegIndex = -1;
        int stringRegIndex = -1;
        int booleanRegIndex = -1;
        for (ColumnDefinition columnDef : columnDefs) {
            String columnName = columnDef.getName();
            TypeKind type = columnDef.getType();
            switch (type) {
            case STRING:
                String sValue = getString(columnName);
                bStruct.setStringField(++stringRegIndex, sValue);
                break;
            case INT:
                long lValue = getInt(columnName);
                bStruct.setIntField(++longRegIndex, lValue);
                break;
            case FLOAT:
                double fValue = getFloat(columnName);
                bStruct.setFloatField(++doubleRegIndex, fValue);
                break;
            case BOOLEAN:
                boolean boolValue = getBoolean(columnName);
                bStruct.setBooleanField(++booleanRegIndex, boolValue ? 1 : 0);
                break;
            default:
                throw new BallerinaException("unsupported sql type found for the column " + columnName);
            }
        }
        return bStruct;
    }

    @Override
    public List<ColumnDefinition> getColumnDefinitions() {
        return this.columnDefs;
    }

    private void generateStructType() {
        BType[] structTypes = new BType[columnDefs.size()];
        BStructType.StructField[] structFields = new BStructType.StructField[columnDefs.size()];
        int typeIndex  = 0;
        for (ColumnDefinition columnDef : columnDefs) {
            BType type;
            switch (columnDef.getType()) {
            case ARRAY:
                type = BTypes.typeMap;
                break;
            case STRING:
                type = BTypes.typeString;
                break;
            case BLOB:
                type = BTypes.typeBlob;
                break;
            case INT:
                type = BTypes.typeInt;
                break;
            case FLOAT:
                type = BTypes.typeFloat;
                break;
            case BOOLEAN:
                type = BTypes.typeBoolean;
                break;
            default:
                type = BTypes.typeNull;
            }
            structTypes[typeIndex] = type;
            structFields[typeIndex] = new BStructType.StructField(type, columnDef.getName());
            ++typeIndex;
        }

        int[] fieldCount = populateMaxSizes(structTypes);
        bStructType = new BStructType("RS", null);
        bStructType.setStructFields(structFields);
        bStructType.setFieldTypeCount(fieldCount);
    }

    private static int[] populateMaxSizes(BType[] paramTypes) {
        int[] maxSizes = new int[6];
        for (int i = 0; i < paramTypes.length; i++) {
            BType paramType = paramTypes[i];
            switch (paramType.getTag()) {
            case TypeTags.INT_TAG:
                ++maxSizes[0];
                break;
            case TypeTags.FLOAT_TAG:
                ++maxSizes[1];
                break;
            case TypeTags.STRING_TAG:
                ++maxSizes[2];
                break;
            case TypeTags.BOOLEAN_TAG:
                ++maxSizes[3];
                break;
            case TypeTags.BLOB_TAG:
                ++maxSizes[4];
                break;
            default:
                ++maxSizes[5];
            }
        }
        return maxSizes;
    }

    private void checkCurrentRow() {
        if (this.current == null) {
            throw new BallerinaException("invalid position in the data iterator");
        }
    }

    private static String getString(ByteBuffer data) {
        if (data == null) {
            return null;
        }
        byte[] encode = getBase64Encode(new String(data.array(), Charset.defaultCharset()));
        return new String(encode, Charset.defaultCharset());
    }

    private static byte[] getBase64Encode(String st) {
        return Base64.getEncoder().encode(st.getBytes(Charset.defaultCharset()));
    }
}
