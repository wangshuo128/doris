// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.qe;

import java.util.UUID;

import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlExecutorTest {
    private static String runningDir;
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        runningDir = "fe/mocked/SqlExecutorTest/" + UUID.randomUUID().toString() + "/";
        UtFrameUtils.createDorisCluster(runningDir);

        connectContext = UtFrameUtils.createDefaultCtx();

        UtFrameUtils.createDb(connectContext, "test");
        connectContext.setDatabase("default_cluster:test");

        String t0 = "create table t0(\n" +
                        "id int, \n" +
                        "k1 int, \n" +
                        "k2 int, \n" +
                        "k3 int, \n" +
                        "v1 int, \n" +
                        "v2 int)\n" +
                        "distributed by hash(k2) buckets 6\n" +
                        "properties('replication_num' = '1');";

        String t1 = "create table t1(\n" +
                        "id int, \n" +
                        "k1 int, \n" +
                        "k2 int, \n" +
                        "k3 int, \n" +
                        "v1 int, \n" +
                        "v2 int)\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "properties('replication_num' = '1');";

        String t2 = "create table t2(\n" +
                        "id int, \n" +
                        "k1 int, \n" +
                        "k2 int, \n" +
                        "k3 int, \n" +
                        "v1 int, \n" +
                        "v2 int)\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "properties('replication_num' = '1');";
        UtFrameUtils.createTables(connectContext, t0, t1, t2);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @Test
    public void test() throws Exception {
        String sql1 = "select id from t0";
        String sql2 = "select t0.id , t0.k1 from t0 join t1 on t0.id=t1.id where t1.k1>0";
        String sql3 = "select t0.id, t1.k1 from t0, t1 join t2  " +
                          "on t1.id=t2.id where t1.id >10";
        String sql4 = "select t0.id , t0.k1 from t0 join t1 on t0.id=t1.id";
        SqlExecutor executor = new SqlExecutor(sql4, connectContext);
        executor.dryRun();
    }
}