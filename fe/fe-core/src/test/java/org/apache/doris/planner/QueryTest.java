// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License

package org.apache.doris.planner;

import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class QueryTest {
    protected static String runningDir;
    protected static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        runningDir = "fe/mocked/QueryTest/" + UUID.randomUUID().toString() + "/";
        UtFrameUtils.createDorisCluster(runningDir);

        connectContext = UtFrameUtils.createDefaultCtx();

        UtFrameUtils.createDb(connectContext, "test");

        String t0 = "create table test.t0(\n" +
                        "id int, \n" +
                        "k1 int, \n" +
                        "k2 int, \n" +
                        "k3 int, \n" +
                        "v1 int, \n" +
                        "v2 int)\n" +
                        "distributed by hash(k2) buckets 1\n" +
                        "properties('replication_num' = '1');";

        String t1 =
            "CREATE TABLE `test`.`t1` (\n" +
                "  `dt` int(11) NULL COMMENT \"\",\n" +
                "  `k1` int(11) NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` int(11) NULL COMMENT \"\",\n" +
                "  `k4` int(11) NULL COMMENT \"\"\n" +
                ") " +
                "DUPLICATE KEY(`dt`, `k1`, `k2`, `k3`, `k4`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p20211121 VALUES LESS THAN (\"20211121\"),\n" +
                "PARTITION p20211122 VALUES [(\"20211121\"), (\"20211122\")),\n" +
                "PARTITION p20211123 VALUES [(\"20211122\"), (\"20211123\")),\n" +
                "PARTITION p20211124 VALUES [(\"20211123\"), (\"20211124\")),\n" +
                "PARTITION p20211125 VALUES [(\"20211124\"), (\"20211125\")),\n" +
                "PARTITION p20211126 VALUES [(\"20211125\"), (\"20211126\")),\n" +
                "PARTITION p20211127 VALUES [(\"20211126\"), (\"20211127\")),\n" +
                "PARTITION p20211128 VALUES [(\"20211127\"), (\"20211128\")))\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 60\n" +
                "PROPERTIES('replication_num' = '1');";

        String t2 =
            "CREATE TABLE `test`.`t2` (\n" +
                "  `dt` int(11) NULL COMMENT \"\",\n" +
                "  `k1` int(11) NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` int(11) NULL COMMENT \"\",\n" +
                "  `k4` int(11) NULL COMMENT \"\"\n" +
                ") " +
                "DUPLICATE KEY(`dt`, `k1`, `k2`, `k3`, `k4`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p20211121 VALUES LESS THAN (\"20211121\"),\n" +
                "PARTITION p20211122 VALUES [(\"20211121\"), (\"20211122\")),\n" +
                "PARTITION p20211123 VALUES [(\"20211122\"), (\"20211123\")),\n" +
                "PARTITION p20211124 VALUES [(\"20211123\"), (\"20211124\")),\n" +
                "PARTITION p20211125 VALUES [(\"20211124\"), (\"20211125\")),\n" +
                "PARTITION p20211126 VALUES [(\"20211125\"), (\"20211126\")),\n" +
                "PARTITION p20211127 VALUES [(\"20211126\"), (\"20211127\")),\n" +
                "PARTITION p20211128 VALUES [(\"20211127\"), (\"20211128\")))\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 60\n" +
                "PROPERTIES('replication_num' = '1');";

        UtFrameUtils.createTables(connectContext, t0, t1, t2);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    /**
     * PLAN FRAGMENT 0
     *  OUTPUT EXPRS:`t1`.`k1` | `t2`.`k2`
     *   PARTITION: UNPARTITIONED
     *
     *   RESULT SINK
     *
     *   4:EXCHANGE
     *      tuple ids: 0 1
     *
     * PLAN FRAGMENT 1
     *  OUTPUT EXPRS:
     *   PARTITION: RANDOM
     *
     *   STREAM DATA SINK
     *     EXCHANGE ID: 04
     *     UNPARTITIONED
     *
     *   2:HASH JOIN
     *   |  join op: INNER JOIN (BROADCAST)
     *   |  hash predicates:
     *   |  colocate: false, reason: Tables are not in the same group
     *   |  equal join conjunct: `t1`.`dt` = `t2`.`dt`
     *   |  runtime filters: RF000[in_or_bloom] <- `t2`.`dt`
     *   |  cardinality=0
     *   |  tuple ids: 0 1
     *   |
     *   |----3:EXCHANGE
     *   |       tuple ids: 1
     *   |
     *   0:OlapScanNode
     *      TABLE: t1
     *      PREAGGREGATION: ON
     *      runtime filters: RF000[in_or_bloom] -> `t1`.`dt`
     *      partitions=8/8
     *      rollup: t1
     *      tabletRatio=480/480
     *      tabletList=10397,10399,10401,10403,10405,10407,10409,10411,10413,10415 ...
     *      cardinality=0
     *      avgRowSize=8.0
     *      numNodes=1
     *      tuple ids: 0
     *
     * PLAN FRAGMENT 2
     *  OUTPUT EXPRS:
     *   PARTITION: RANDOM
     *
     *   STREAM DATA SINK
     *     EXCHANGE ID: 03
     *     UNPARTITIONED
     *
     *   1:OlapScanNode
     *      TABLE: t2
     *      PREAGGREGATION: ON
     *      partitions=8/8
     *      rollup: t2
     *      tabletRatio=480/480
     *      tabletList=11727,11729,11731,11733,11735,11737,11739,11741,11743,11745 ...
     *      cardinality=0
     *      avgRowSize=8.0
     *      numNodes=1
     *      tuple ids: 1
     *
     * Tuples:
     * TupleDescriptor{id=0, tbl=t1, byteSize=12, materialized=true}
     *   SlotDescriptor{id=0, col=dt, type=INT}
     *     parent=0
     *     materialized=true
     *     byteSize=4
     *     byteOffset=4
     *     nullIndicatorByte=0
     *     nullIndicatorBit=0
     *     slotIdx=0
     *
     *   SlotDescriptor{id=2, col=k1, type=INT}
     *     parent=0
     *     materialized=true
     *     byteSize=4
     *     byteOffset=8
     *     nullIndicatorByte=0
     *     nullIndicatorBit=1
     *     slotIdx=1
     *
     *
     * TupleDescriptor{id=1, tbl=t2, byteSize=12, materialized=true}
     *   SlotDescriptor{id=1, col=dt, type=INT}
     *     parent=1
     *     materialized=true
     *     byteSize=4
     *     byteOffset=4
     *     nullIndicatorByte=0
     *     nullIndicatorBit=0
     *     slotIdx=0
     *
     *   SlotDescriptor{id=3, col=k2, type=INT}
     *     parent=1
     *     materialized=true
     *     byteSize=4
     *     byteOffset=8
     *     nullIndicatorByte=0
     *     nullIndicatorBit=1
     *     slotIdx=1
     */
    @Test
    public void testJoin() throws Exception {
        String sql = "select t1.k1, t2.k2 from test.t1 t1 join test.t2 t2 on t1.dt=t2.dt";
        String msg = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql, true);
        System.out.println(msg);
    }

    @Test
    public void testSelectStar() throws Exception {
        String sql = "select * from test.t1 t1 join test.t2 t2 on t1.dt=t2.dt";
        String msg = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql, true);
        System.out.println(msg);
    }

    /**
     * PLAN FRAGMENT 0
     *  OUTPUT EXPRS:`id`
     *   PARTITION: UNPARTITIONED
     *
     *   RESULT SINK
     *
     *   1:EXCHANGE
     *      tuple ids: 0
     *
     * PLAN FRAGMENT 1
     *  OUTPUT EXPRS:
     *   PARTITION: HASH_PARTITIONED: `default_cluster:test`.`t0`.`k2`
     *
     *   STREAM DATA SINK
     *     EXCHANGE ID: 01
     *     UNPARTITIONED
     *
     *   0:OlapScanNode
     *      TABLE: t0
     *      PREAGGREGATION: ON
     *      partitions=1/1
     *      rollup: t0
     *      tabletRatio=10/10
     *      tabletList=10007,10009,10011,10013,10015,10017,10019,10021,10023,10025
     *      cardinality=0
     *      avgRowSize=4.0
     *      numNodes=1
     *      tuple ids: 0
     *
     * Tuples:
     * TupleDescriptor{id=0, tbl=t0, byteSize=8, materialized=true}
     *   SlotDescriptor{id=0, col=id, type=INT}
     *     parent=0
     *     materialized=true
     *     byteSize=4
     *     byteOffset=4
     *     nullIndicatorByte=0
     *     nullIndicatorBit=0
     *     slotIdx=0
     */
    @Test
    public void testTableScan() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String sql = "select id from t0";
        String msg = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql, true);
        System.out.println(msg);
    }
}
