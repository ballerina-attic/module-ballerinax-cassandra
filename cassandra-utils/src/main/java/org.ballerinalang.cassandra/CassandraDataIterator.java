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
import org.ballerinalang.jvm.ColumnDefinition;
import org.ballerinalang.jvm.DataIterator;
import org.ballerinalang.jvm.types.BField;
import org.ballerinalang.jvm.types.BStructureType;
import org.ballerinalang.jvm.types.TypeTags;
import org.ballerinalang.jvm.values.DecimalValue;
import org.ballerinalang.jvm.values.MapValue;
import org.ballerinalang.jvm.values.MapValueImpl;
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

    private BStructureType bStructType;

    public CassandraDataIterator(ResultSet rs, List<ColumnDefinition> columnDefs, BStructureType recordType) {
        this.iterator = rs.iterator();
        this.columnDefs = columnDefs;
        this.bStructType = recordType;
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
    public void close() {
        /* ignore */
    }

    @Override
    public void reset() {
        /* ignore */
    }

    @Override
    public String getString(int columnIndex) {
        this.checkCurrentRow();
        return this.current.getString(columnIndex - 1);
    }

    @Override
    public Long getInt(int columnIndex) {
        this.checkCurrentRow();
        return (long) this.current.getInt(columnIndex - 1);
    }

    @Override
    public Double getFloat(int columnIndex) {
        this.checkCurrentRow();
        return (double) this.current.getFloat(columnIndex - 1);
    }

    @Override
    public Boolean getBoolean(int columnIndex) {
        this.checkCurrentRow();
        return this.current.getBool(columnIndex - 1);
    }

    @Override
    public String getBlob(int columnIndex) {
        this.checkCurrentRow();
        return getString(this.current.getBytes(columnIndex - 1));
    }

    @Override
    public DecimalValue getDecimal(int i) {
        return null;
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
    public MapValue<String, Object> generateNext() {
        MapValue<String, Object> bStruct = new MapValueImpl<>();
        Map<String, BField> recordFields = this.bStructType.getFields();
        int index = 0;
        for (ColumnDefinition columnDef : columnDefs) {
            String columnName = columnDef.getName();
            int type = columnDef.getTypeTag();
            String fieldName = recordFields.values().toArray(new BField[0])[index].getFieldName();
            ++index;
            switch (type) {
            case TypeTags.STRING_TAG:
                String sValue = getString(index);
                bStruct.put(fieldName, sValue);
                break;
                case TypeTags.INT_TAG:
                long lValue = getInt(index);
                bStruct.put(fieldName, lValue);
                break;
            case TypeTags.FLOAT_TAG:
                double fValue = getFloat(index);
                bStruct.put(fieldName, fValue);
                break;
            case TypeTags.BOOLEAN_TAG:
                boolean boolValue = getBoolean(index);
                bStruct.put(fieldName, boolValue);
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
