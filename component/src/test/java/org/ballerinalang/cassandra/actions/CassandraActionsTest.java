/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BFloat;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BValue;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * This class contains test cases for SELECT, UPDATE actions.
 */
public class CassandraActionsTest extends CassandraBaseTest {
    private static CompileResult result;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        result = BCompileUtil.compile("samples/cassandra-actions-test.bal");
        populateDatabase();
    }

    public void populateDatabase() {
        PreparedStatement keySpaceCreateStmt = session.prepare(
                "CREATE KEYSPACE peopleinfoks  WITH replication = {'class':'SimpleStrategy', 'replication_factor' : "
                        + "1}");
        session.execute(keySpaceCreateStmt.bind());
        PreparedStatement createTableStmt = session.prepare("CREATE TABLE peopleinfoks.person(id int, name "
                + "text,salary float,income double, married boolean, PRIMARY KEY (id, name, salary, income, married))");
        session.execute(createTableStmt.bind());
        PreparedStatement insertStmt = session.prepare(
                "insert into peopleinfoks.person (id, name, salary, income, married) values (1, 'Jack', 100.2, "
                        + "10000.5, true)");
        session.execute(insertStmt.bind());

    }

    @Test(description = "This method tests Cassandra key space creation")
    public void testKeySpaceCreation() throws Exception {
        BRunUtil.invoke(result, "testKeySpaceCreation");
        Session session = null;
        try {
            session = cluster.connect("dummyks");
            Assert.assertEquals(session.getLoggedKeyspace(), "dummyks");
        } catch (InvalidQueryException e) {
            Assert.fail("KeySpace generation might have failed");
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Test(description = "This method tests Cassandra update failure scenario")
    public void testUpdateExceptionScenario() throws Exception {
        BValue[] results = BRunUtil.invoke(result, "testDuplicateKeySpaceCreation");
        Assert.assertNotNull(results);
        Assert.assertTrue(results[0].stringValue().contains("Keyspace duplicatekstest already exists"));

    }

    @Test(description = "This method tests table creation in a Cassandra database")
    public void testTableCreation() throws Exception {
        BRunUtil.invoke(result, "testTableCreation");
        PreparedStatement selectStmt = session.prepare("select * from peopleinfoks.student");
        ResultSet resultSet = session.execute(selectStmt.bind());
        List<Row> resultList = resultSet.all();
        Assert.assertEquals(resultList.size(), 0, "An empty result set should have been retrieved");
    }

    @Test(description = "This method tests insertion of raw params to Cassandra database")
    public void testInsertRawParams() throws Exception {
        BRunUtil.invoke(result, "testInsertRawParams");
        PreparedStatement selectStmt = session.prepare("select * from peopleinfoks.person where id = 10");
        ResultSet resultSet = session.execute(selectStmt.bind());
        List<Row> resultList = resultSet.all();
        boolean found = false;
        for (Row row : resultList) {
            if (row.get("id", Integer.class) == 10 && "Tommy".equals(row.get("name", String.class))
                    && Float.compare(row.get("salary", Float.class), 101.5f) == 0
                    && Double.compare(row.get("income", Double.class), 1001.5) == 0 && !row
                    .get("married", Boolean.class)) {
                found = true;
            }
        }
        Assert.assertTrue(found, "The data might not have been inserted");
    }

    @Test(description = "This method tests selection from Cassandra database")
    public void testSelect() throws Exception {
        BValue[] results = BRunUtil.invoke(result, "testSelect");
        long id = ((BInteger) results[0]).intValue();
        String name = results[1].stringValue();
        float salary = (float) ((BFloat) results[2]).floatValue();
        Assert.assertTrue(id == 1 && "Jack".equals(name) && Float.compare(salary, 100.2f) == 0,
                "Retrieved data is incorrect");
    }

    @Test(description = "This method tests selection from Cassandra database with nil param array")
    public void testSelectWithNilParams() throws Exception {
        BValue[] results = BRunUtil.invoke(result, "testSelectWithNilParams");
        long id = ((BInteger) results[0]).intValue();
        String name = results[1].stringValue();
        float salary = (float) ((BFloat) results[2]).floatValue();
        Assert.assertTrue(id == 1 && "Jack".equals(name) && Float.compare(salary, 100.2f) == 0,
                "Retrieved data is incorrect");
    }

    @Test(description = "This method tests selection failure from Cassandra database")
    public void testSelectExceptionScenario() throws Exception {
        BValue[] results = BRunUtil.invoke(result, "testSelectNonExistentColumn");
        Assert.assertNotNull(results);
        Assert.assertTrue(results[0].stringValue().contains("Undefined column name x"));
    }

    @Test(description = "This method tests select action with parameter arrays")
    public void testSelectWithParamArray() throws Exception {
        BValue[] results = BRunUtil.invoke(result, "testSelectWithParamArray");
        long id = ((BInteger) results[0]).intValue();
        String name = results[1].stringValue();
        float salary = (float) ((BFloat) results[2]).floatValue();
        boolean married = ((BBoolean) results[3]).booleanValue();
        Assert.assertTrue(id == 1 && "Jack".equals(name) && Float.compare(salary, 100.2f) == 0 && married,
                "Retrieved data is incorrect");
    }

}
