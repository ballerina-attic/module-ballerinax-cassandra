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

package org.ballerinalang.cassandra;

import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.model.types.BType;
import org.ballerinalang.model.types.TypeTags;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.util.codegen.PackageInfo;
import org.ballerinalang.util.codegen.StructInfo;
import org.ballerinalang.util.exceptions.BallerinaException;

import static org.ballerinalang.bre.bvm.BLangVMErrors.PACKAGE_BUILTIN;

/**
 * This class contains util methods required for Cassandra ballerina package.
 */
public class CassandraDataSourceUtils {
    public static BStruct getCassandraConnectorError(Context context, Throwable throwable) {
        PackageInfo builtinPackage = context.getProgramFile().getPackageInfo(PACKAGE_BUILTIN);
        StructInfo errorStructInfo = builtinPackage.getStructInfo(BLangVMErrors.STRUCT_GENERIC_ERROR);
        BStruct cassandraConnectorError = new BStruct(errorStructInfo.getType());
        if (throwable.getMessage() == null) {
            cassandraConnectorError.setStringField(0, Constants.CASSANDRA_EXCEPTION_OCCURED);
        } else {
            cassandraConnectorError.setStringField(0, throwable.getMessage());
        }
        return cassandraConnectorError;
    }

    public static String getCQLType(BType value) {
        int tag = value.getTag();
        switch (tag) {
        case TypeTags.INT_TAG:
            return Constants.DataTypes.INT;
        case TypeTags.STRING_TAG:
            return Constants.DataTypes.TEXT;
        case TypeTags.FLOAT_TAG:
            return Constants.DataTypes.FLOAT;
        case TypeTags.BOOLEAN_TAG:
            return Constants.DataTypes.BOOLEAN;
        default:
            throw new BallerinaException("unsupported data type for record field: " + value.getName());
        }
    }
}
