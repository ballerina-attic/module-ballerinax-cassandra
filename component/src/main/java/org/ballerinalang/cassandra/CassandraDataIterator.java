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
package org.ballerinalang.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.ballerinalang.model.ColumnDefinition;
import org.ballerinalang.model.DataIterator;
import org.ballerinalang.model.types.BField;
import org.ballerinalang.model.types.BStructureType;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BFloat;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

/**
 * This iterator wraps a cassandra data row.
 *
 * @since 0.95.0
 */
public class CassandraDataIterator implements DataIterator {

    private Iterator<Row> iterator;

    private Row current;

    private List<ColumnDefinition> columnDefs;

    private BStructureType bStructType;

    public CassandraDataIterator(ResultSet rs, List<ColumnDefinition> columnDefs, BStructureType structType) {
        this.iterator = rs.iterator();
        this.columnDefs = columnDefs;
        this.bStructType = structType;
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
    public void reset(boolean b) {
        /* ignore */
    }

    @Override
    public String getString(int columnIndex) {
        this.checkCurrentRow();
        return this.current.getString(columnIndex - 1);
    }

    @Override
    public long getInt(int columnIndex) {
        this.checkCurrentRow();
        return this.current.getInt(columnIndex - 1);
    }

    @Override
    public double getFloat(int columnIndex) {
        this.checkCurrentRow();
        return this.current.getFloat(columnIndex - 1);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        this.checkCurrentRow();
        return this.current.getBool(columnIndex - 1);
    }

    @Override
    public String getBlob(int columnIndex) {
        this.checkCurrentRow();
        return getString(this.current.getBytes(columnIndex - 1));
    }

    @Override
    public Object[] getStruct(int i) {
        return new Object[0];
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS",
                        justification = "Functionality of obtaining an array is not implemented therefore it would be"
                                + " incorrect to return an empty array")
    @Override
    public Object[] getArray(int columnIndex) {
        return null;
    }

    @Override
    public BMap<String, BValue> generateNext() {
        BMap<String, BValue> bStruct = new BMap<>(bStructType);
        BField[] recordFields = this.bStructType.getFields();
        int index = 0;
        for (ColumnDefinition columnDef : columnDefs) {
            String columnName = columnDef.getName();
            TypeKind type = columnDef.getType();
            String fieldName = recordFields[index].getFieldName();
            ++index;
            switch (type) {
            case STRING:
                String sValue = getString(index);
                bStruct.put(fieldName, new BString(sValue));
                break;
            case INT:
                long lValue = getInt(index);
                bStruct.put(fieldName, new BInteger(lValue));
                break;
            case FLOAT:
                double fValue = getFloat(index);
                bStruct.put(fieldName, new BFloat(fValue));
                break;
            case BOOLEAN:
                boolean boolValue = getBoolean(index);
                bStruct.put(fieldName, new BBoolean(boolValue));
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

    @Override
    public BStructureType getStructType() {
        return this.bStructType;
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
