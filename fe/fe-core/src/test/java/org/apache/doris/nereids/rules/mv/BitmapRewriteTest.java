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

package org.apache.doris.nereids.rules.mv;

import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class BitmapRewriteTest extends TestWithFeService {
    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        useDatabase("test");

        createTable("CREATE TABLE `t` (\n"
                + "  k1 int,\n"
                + "  v1 bitmap BITMAP_UNION \n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(k1)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ")");

        // createTable("CREATE TABLE `t1` (\n"
        //         + "  k1 int,\n"
        //         + "  k2 int,\n"
        //         + "  k3 int,\n"
        //         + "  k4 int,\n"
        //         + "  v1 int\n"
        //         + ") ENGINE=OLAP\n"
        //         + "COMMENT \"OLAP\"\n"
        //         + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
        //         + "PROPERTIES (\n"
        //         + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
        //         + "\"in_memory\" = \"false\",\n"
        //         + "\"storage_format\" = \"V2\",\n"
        //         + "\"disable_auto_compaction\" = \"false\"\n"
        //         + ")");
        createTable("create table t1(\n"
                + "  k1 int, \n"
                + "  k2 int, \n"
                + "  v1 int\n"
                + ")ENGINE=OLAP \n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
    }

    @Test
    public void useMv() throws Exception {
        // createMv("create materialized view mv1 as "
        //         + "select k1, bitmap_union(to_bitmap(v1)) from t1 group by k1");
        createMv("\n"
                + "create materialized view mv1 as \n"
                + "  select k1, bitmap_union(to_bitmap(v1)) from t1 group by k1;");
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1, count(distinct v1) from t1 group by k1", planner -> {
                    List<ScanNode> scans = planner.getScanNodes();
                    Assertions.assertEquals(1, scans.size());
                    ScanNode scanNode = scans.get(0);
                    Assertions.assertTrue(scanNode instanceof OlapScanNode);
                    OlapScanNode olapScan = (OlapScanNode) scanNode;
                    String indexName = olapScan.getSelectedIndexName();
                    System.out.println("index name: " + indexName);
                });
    }

    @Test
    public void mvIndexBitmapMissingKey() throws Exception {
        createMv("create materialized view mv1 as "
                + "select k2, bitmap_union(to_bitmap(v1)) from t1 group by k2");

        String explain = getSQLPlanOrErrorMsg("select k1, count(distinct v1) from t1 group by k1");
        System.out.println(explain);
    }

    @Test
    public void test() throws Exception {
        String explain = getSQLPlanOrErrorMsg("select k1, count(distinct v1) from t group by k1");
        System.out.println(explain);
    }

    // physical plan:
    // PhysicalProject ( projects=[k1#0, count(distinct v1)#3 AS `count(DISTINCT v1)`#2], stats=(rows=1, isReduced=true, width=1, penalty=0.0) )
    //         +--PhysicalAggregate ( phase=LOCAL, outputExpr=[k1#0, count(distinct v1#1) AS `count(distinct v1)`#3], groupByExpr=[k1#0], partitionExpr=[k1#0], stats=(rows=1, isReduced=true, width=1, penalty=0.0) )
    //         +--PhysicalOlapScan ( qualified=default_cluster:test.t, output=[k1#0, v1#1], stats=(rows=0, isReduced=false, width=1, penalty=0.0) )
    @Test
    public void countDistinctTest() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1, count(distinct v1) from t group by k1");
    }

    @Test
    public void bitmapUnionCountTest() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1, bitmap_union_count(v1) from t group by k1");
    }

    @Test
    public void testDetailCount() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select count(distinct v1 + 1) as cnt from t1 group by k1");
    }
}
