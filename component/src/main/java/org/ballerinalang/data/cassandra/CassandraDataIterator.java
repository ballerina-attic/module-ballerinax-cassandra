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
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.Iterator;
import java.util.Map;

/**
 * This iterator mainly wrap cassandra row.
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
    public boolean isLast() {
        //TODO
        return false;
    }

    @Override
    public void close(boolean b) {
        //TODO
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
    public BValue get(String s, int i) {
        //TODO
        return null;
    }

    @Override
    public Map<String, Object> getArray(String s) {
        //TODO
        return null;
    }

    private void checkCurrentRow() {
        if (this.current == null) {
            throw new BallerinaException("invalid position in the data iterator");
        }
    }
}
