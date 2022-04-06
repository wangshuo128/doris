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


package org.apache.doris.planner;

import java.util.UUID;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SubqueryTest {


    protected static String runningDir;
    protected static ConnectContext connectContext;

    protected static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        runningDir = "fe/mocked/SubqueryTest/" + UUID.randomUUID().toString() + "/";
        UtFrameUtils.createDorisCluster(runningDir);

        connectContext = UtFrameUtils.createDefaultCtx();

        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);

        String t1 =
            "CREATE TABLE `test`.`t1` (\n" +
                "  `id1` int(11) NULL COMMENT \"\",\n" +
                "  `k1` int(11) NULL COMMENT \"\"\n" +
                ") " +
                "DUPLICATE KEY(`id1`, `k1`)\n" +
                "DISTRIBUTED BY HASH(`id1`) BUCKETS 60\n" +
                "PROPERTIES('replication_num' = '1');";


        String t2 =
            "CREATE TABLE `test`.`t2` (\n" +
                "  `id2` int(11) NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\"\n" +
                ") " +
                "DUPLICATE KEY(`id2`, `k2`)\n" +
                "DISTRIBUTED BY HASH(`id2`) BUCKETS 60\n" +
                "PROPERTIES('replication_num' = '1');";

        String t3 =
            "CREATE TABLE `test`.`t3` (\n" +
                "  `id3` int(11) NULL COMMENT \"\",\n" +
                "  `k3` int(11) NULL COMMENT \"\"\n" +
                ") " +
                "DUPLICATE KEY(`id3`, `k3`)\n" +
                "DISTRIBUTED BY HASH(`id3`) BUCKETS 60\n" +
                "PROPERTIES('replication_num' = '1');";


        createTable(t1);
        createTable(t2);
        createTable(t3);
    }

    @AfterClass
    public static void tearDown() throws Exception {

        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @Test
    public void test() throws Exception {
        String sql = "select * from test.t1\n" +
                         "where id1 in (\n" +
                         "    select id2 from test.t2 \n" +
                         "    where id2 in (\n" +
                         "        select id3 from  test.t3 \n" +
                         "        where id3=id1\n" +
                         "    )\n" +
                         ");\n" +
                         "\n";


        String sql1 = "select * from test.t1 a\n" +
                          "where k1 in (\n" +
                          "    select k1 from test.t1 b \n" +
                          "    where k1 in (\n" +
                          "        select k1 from  test.t1 c \n" +
                          "        where a.k1=k1\n" +
                          "    )\n" +
                          ");";
        // String msg = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql);
        // String msg = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql1);
        String sql2 = "select * from test.tt";
        String msg = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql2);
        System.out.println(msg);
    }
}
