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
import org.ballerinalang.model.DataIterator;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BDataTable;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This iterator wraps a cassandra row.
 */
public class CassandraDataIterator implements DataIterator {

    private Iterator<Row> iterator;

    private Row current;

    public CassandraDataIterator(ResultSet rs) {
        this.iterator = rs.iterator();
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
    public String getObjectAsString(String s) {
        this.checkCurrentRow();
        return this.current.getObject(s).toString();
    }

    @Override
    public Map<String, Object> getArray(String s) {
        return null;
    }

    @Override
    public void generateNext(List<BDataTable.ColumnDefinition> columnDefs, BStruct bStruct) {
        int longRegIndex = -1;
        int doubleRegIndex = -1;
        int stringRegIndex = -1;
        int booleanRegIndex = -1;
        for (BDataTable.ColumnDefinition columnDef : columnDefs) {
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
    }

    private void checkCurrentRow() {
        if (this.current == null) {
            throw new BallerinaException("invalid position in the data iterator");
        }
    }
}
