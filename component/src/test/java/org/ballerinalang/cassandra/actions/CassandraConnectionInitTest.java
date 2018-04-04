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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.util.exceptions.BLangRuntimeException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CassandraConnectionInitTest extends CassandraBaseTest {
    private static CompileResult result;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        result = BCompileUtil.compile("samples/cassandra-connection-init-test.bal");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with a Load Balancing Policy")
    public void testConnectionInitWithLBPolicy() throws Exception {
        testConnectionInitByKeySpaceGeneration("testConnectionInitWithLBPolicy", "lbtestkeyspace");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with a Retry Policy")
    public void testConnectionInitWithRetryPolicy() throws Exception {
        testConnectionInitByKeySpaceGeneration("testConnectionInitWithRetryPolicy", "retrytestkeyspace");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with a Reconnection Policy")
    public void testConnectionInitWithReconnectionPolicy() throws Exception {
        testConnectionInitByKeySpaceGeneration("testConnectionInitWithReconnectionPolicy", "reconnectiontestkeyspace");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with Pooling Options")
    public void testConnectionInitWithPoolingOptions() throws Exception {
        testConnectionInitByKeySpaceGeneration("testConnectionInitWithPoolingOptions", "poolingoptionstestkeyspace");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with Socket Options")
    public void testConnectionInitWithSocketOptions() throws Exception {
        testConnectionInitByKeySpaceGeneration("testConnectionInitWithSocketOptions", "socketoptionstestkeyspace");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with Query Options")
    public void testConnectionInitWithQueryOptions() throws Exception {
        testConnectionInitByKeySpaceGeneration("testConnectionInitWithQueryOptions", "queryoptionstestkeyspace");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with Protocol Options")
    public void testConnectionInitWithProtocolOptions() throws Exception {
        testConnectionInitByKeySpaceGeneration("testConnectionInitWithProtocolOptions", "protocoloptionstestkeyspace");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with additional connection parameters")
    public void testConnectionInitWithAdditionalConParams() throws Exception {
        testConnectionInitByKeySpaceGeneration("testConnectionInitWithAdditionalConnectionParams",
                "conparamtestkeyspace");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with an invalid Load Balancing Policy",
          expectedExceptions = { BLangRuntimeException.class })
    public void testConnectionInitWithInvalidLBPolicy() throws Exception {
        BRunUtil.invoke(result, "testConnectionInitWithInvalidLBPolicy");
        Assert.fail("BLangRuntimeException should have been thrown by this point");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with an invalid Retry Policy",
          expectedExceptions = { BLangRuntimeException.class })
    public void testConnectionInitWithInvalidRetryPolicy() throws Exception {
        BRunUtil.invoke(result, "testConnectionInitWithInvalidRetryPolicy");
        Assert.fail("BLangRuntimeException should have been thrown by this point");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with an invalid Reconnection Policy",
          expectedExceptions = { BLangRuntimeException.class })
    public void testConnectionInitWithInvalidReconnectionPolicy() throws Exception {
        BRunUtil.invoke(result, "testConnectionInitWithInvalidReconnectionPolicy");
        Assert.fail("BLangRuntimeException should have been thrown by this point");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with an invalid Consistency Level",
          expectedExceptions = { BLangRuntimeException.class })
    public void testConnectionInitWithInvalidConsistencyLevel() throws Exception {
        BRunUtil.invoke(result, "testConnectionInitWithInvalidConsistencyLevel");
        Assert.fail("BLangRuntimeException should have been thrown by this point");
    }

    @Test(description = "This method tests Cassandra Connection Inilialization with an invalid Serial Consistency "
            + "Level", expectedExceptions = { BLangRuntimeException.class })
    public void testConnectionInitWithInvalidSerialConsistencyLevel() throws Exception {
        BRunUtil.invoke(result, "testConnectionInitWithInvalidSerialConsistencyLevel");
        Assert.fail("BLangRuntimeException should have been thrown by this point");
    }

    private void testConnectionInitByKeySpaceGeneration(String function, String keyspace) {
        BRunUtil.invoke(result, function);
        Session session = null;
        try {
            session = cluster.connect(keyspace);
            Assert.assertEquals(session.getLoggedKeyspace(), keyspace);
        } catch (InvalidQueryException e) {
            Assert.fail("KeySpace generation might have failed");
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
