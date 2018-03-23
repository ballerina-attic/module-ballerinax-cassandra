/*
 * Copyright (c) 2018, WSO2 Inc. (http:www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http:www.apache.orglicensesLICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.data.cassandra;

import org.ballerinalang.bre.Context;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.util.codegen.PackageInfo;
import org.ballerinalang.util.codegen.StructInfo;

/**
 * This class contains util methods required for Cassandra ballerina package.
 */
public class CassandraDataSourceUtils {
    public static BStruct getCassandraConnectorError(Context context, Throwable throwable) {
        PackageInfo cassandraPackageInfo = context.getProgramFile().getPackageInfo(Constants.CASSANDRA_PACKAGE_PATH);
        StructInfo errorStructInfo = cassandraPackageInfo.getStructInfo(Constants.CASSANDRA_CONNECTOR_ERROR);
        BStruct cassandraConnectorError = new BStruct(errorStructInfo.getType());
        if (throwable.getMessage() == null) {
            cassandraConnectorError.setStringField(0, Constants.CASSANDRA_EXCEPTION_OCCURED);
        } else {
            cassandraConnectorError.setStringField(0, throwable.getMessage());
        }
        return cassandraConnectorError;
    }
}
