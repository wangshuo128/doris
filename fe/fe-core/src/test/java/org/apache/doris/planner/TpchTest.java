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

import com.google.common.base.MoreObjects;
import org.apache.commons.io.FileUtils;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import scala.collection.immutable.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TpchTest {
    private static final Logger logger = LoggerFactory.getLogger(TpchTest.class);

    protected static String runningDir;
    protected static ConnectContext connectContext;

    private static final String supplier =
        "CREATE TABLE tpch.supplier ( S_SUPPKEY     INTEGER NOT NULL,\n" +
            "                        S_NAME        CHAR(25) NOT NULL,\n" +
            "                        S_ADDRESS     VARCHAR(40) NOT NULL,\n" +
            "                        S_NATIONKEY   INTEGER NOT NULL,\n" +
            "                        S_PHONE      CHAR(15) NOT NULL,\n" +
            "                        S_ACCTBAL     DECIMAL(15,2) NOT NULL,\n" +
            "                        S_COMMENT     VARCHAR(101) NOT NULL\n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY (S_SUPPKEY)\n" +
            "DISTRIBUTED BY HASH(S_SUPPKEY) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"V2\"\n" +
            ");";
    private static final String region =
        "CREATE TABLE tpch.region  ( R_REGIONKEY  INTEGER NOT NULL,\n" +
            "                       R_NAME       CHAR(25) NOT NULL,\n" +
            "                       R_COMMENT    VARCHAR(152)\n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY (R_REGIONKEY)\n" +
            "DISTRIBUTED BY HASH(R_REGIONKEY) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"V2\"\n" +
            ");";
    private static final String part =
        "CREATE TABLE tpch.part  (  P_PARTKEY     INTEGER NOT NULL,\n" +
            "                      P_NAME        VARCHAR(55) NOT NULL,\n" +
            "                      P_MFGR        CHAR(25) NOT NULL,\n" +
            "                      P_BRAND       CHAR(10) NOT NULL,\n" +
            "                      P_TYPE        VARCHAR(25) NOT NULL,\n" +
            "                      P_SIZE        INTEGER NOT NULL,\n" +
            "                      P_CONTAINER   CHAR(10) NOT NULL,\n" +
            "                      P_RETAILPRICE DECIMAL(15,2) NOT NULL,\n" +
            "                      P_COMMENT     VARCHAR(23) NOT NULL \n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY (P_PARTKEY)\n" +
            "DISTRIBUTED BY HASH(P_PARTKEY) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"V2\"\n" +
            ");";
    private static final String nation =
        "CREATE TABLE tpch.nation  (N_NATIONKEY  INTEGER NOT NULL,\n" +
            "                      N_NAME       CHAR(25) NOT NULL,\n" +
            "                      N_REGIONKEY  INTEGER NOT NULL,\n" +
            "                      N_COMMENT    VARCHAR(152)\n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY (N_NATIONKEY)\n" +
            "DISTRIBUTED BY HASH(N_NATIONKEY) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"V2\"\n" +
            ");";
    private static final String partsupp =
        "CREATE TABLE tpch.partsupp ( PS_PARTKEY     INTEGER NOT NULL,\n" +
            "                        PS_SUPPKEY     INTEGER NOT NULL,\n" +
            "                        PS_AVAILQTY    INTEGER NOT NULL,\n" +
            "                        PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,\n" +
            "                        PS_COMMENT     VARCHAR(199) NOT NULL \n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY (PS_PARTKEY)\n" +
            "DISTRIBUTED BY HASH(PS_PARTKEY) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"V2\"\n" +
            ");";
    private static final String customer =
        "CREATE TABLE tpch.customer (    C_CUSTKEY     INTEGER NOT NULL,\n" +
            "                           C_NAME        VARCHAR(25) NOT NULL,\n" +
            "                           C_ADDRESS     VARCHAR(40) NOT NULL,\n" +
            "                           C_NATIONKEY   INTEGER NOT NULL,\n" +
            "                           C_PHONE       CHAR(15) NOT NULL,\n" +
            "                           C_ACCTBAL     DECIMAL(15,2)   NOT NULL,\n" +
            "                           C_MKTSEGMENT  CHAR(10) NOT NULL,\n" +
            "                           C_COMMENT     VARCHAR(117) NOT NULL\n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY (C_CUSTKEY)\n" +
            "DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"V2\"\n" +
            ");";
    private static final String orders =
        "CREATE TABLE tpch.orders  (   O_ORDERKEY       INTEGER NOT NULL,\n" +
            "                         O_CUSTKEY        INTEGER NOT NULL,\n" +
            "                         O_ORDERSTATUS    CHAR(1) NOT NULL,\n" +
            "                         O_TOTALPRICE     DECIMAL(15,2) NOT NULL,\n" +
            "                         O_ORDERDATE      DATE NOT NULL,\n" +
            "                         O_ORDERPRIORITY  CHAR(15) NOT NULL,\n" +
            "                         O_CLERK          CHAR(15) NOT NULL,\n" +
            "                         O_SHIPPRIORITY   INTEGER NOT NULL,\n" +
            "                         O_COMMENT        VARCHAR(79) NOT NULL\n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY (O_ORDERKEY)\n" +
            "DISTRIBUTED BY HASH(O_ORDERKEY) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"V2\"\n" +
            ");";
    private static final String lineitem =
        "CREATE TABLE tpch.lineitem (    L_ORDERKEY    INTEGER NOT NULL,\n" +
            "                           L_PARTKEY     INTEGER NOT NULL,\n" +
            "                           L_SUPPKEY     INTEGER NOT NULL,\n" +
            "                           L_LINENUMBER  INTEGER NOT NULL,\n" +
            "                           L_QUANTITY    DECIMAL(15,2) NOT NULL,\n" +
            "                           L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,\n" +
            "                           L_DISCOUNT    DECIMAL(15,2) NOT NULL,\n" +
            "                           L_TAX         DECIMAL(15,2) NOT NULL,\n" +
            "                           L_RETURNFLAG  CHAR(1) NOT NULL,\n" +
            "                           L_LINESTATUS  CHAR(1) NOT NULL,\n" +
            "                           L_SHIPDATE    DATE NOT NULL,\n" +
            "                           L_COMMITDATE  DATE NOT NULL,\n" +
            "                           L_RECEIPTDATE DATE NOT NULL,\n" +
            "                           L_SHIPINSTRUCT CHAR(25) NOT NULL,\n" +
            "                           L_SHIPMODE     CHAR(10) NOT NULL,\n" +
            "                           L_COMMENT      VARCHAR(44) NOT NULL\n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY (L_ORDERKEY)\n" +
            "DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"V2\"\n" +
            ");";

    private static final String[] tables = new String[]{supplier, region, part, nation, partsupp, customer, orders, lineitem};

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        runningDir = "fe/mocked/TpchTest/" + UUID.randomUUID().toString() + "/";
        UtFrameUtils.createDorisCluster(runningDir);

        connectContext = UtFrameUtils.createDefaultCtx();

        String createDbStmtStr = "create database tpch;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        for (String table : tables) {
            System.out.println("create table\n" + table);
            logger.info("create table {}", table);
            createTable(table);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    protected static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }
//
//     @Test
//     public void test() throws Exception {
//         String sql = "explain\n" +
//                          "select \n" +
//                          "l_orderkey,\n" +
//                          "l_linenumber,\n" +
//                          "o_orderkey\n" +
//                          " from \n" +
//                          "tpch.lineitem join tpch.orders \n" +
//                          "on l_orderkey = o_orderkey \n" +
//                          "where o_orderkey<\"1997-04-08\"";
//
//         String q1 = "select \n" +
//                         "    l_returnflag, --返回标志\n" +
//                         "    l_linestatus, \n" +
//                         "    sum(l_quantity) as sum_qty, --总的数量\n" +
//                         "    sum(l_extendedprice) as sum_base_price, --聚集函数操作\n" +
//                         "    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, \n" +
//                         "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, \n" +
//                         "    avg(l_quantity) as avg_qty, \n" +
//                         "    avg(l_extendedprice) as avg_price, \n" +
//                         "    avg(l_discount) as avg_disc, \n" +
//                         "    count(*) as count_order --每个分组所包含的行数\n" +
//                         "from \n" +
//                         "    tpch.lineitem\n" +
//                         "where \n" +
//                         "    l_shipdate <= date'1998-12-01' - interval '90' day --时间段是随机生成的\n" +
//                         "group by --分组操作\n" +
//                         "    l_returnflag, \n" +
//                         "    l_linestatus\n" +
//                         "order by --排序操作\n" +
//                         "    l_returnflag, \n" +
//                         "    l_linestatus;";
//
//         String q5 =
//             "select\n" +
//                 "\tn_name,\n" +
//                 "\tsum(l_extendedprice * (1 - l_discount)) as revenue \n" +
//                 "from\n" +
//                 "\ttpch.customer, tpch.orders, tpch.lineitem, tpch.supplier, tpch.nation, tpch.region \n" +
//                 "where\n" +
//                 "\tc_custkey = o_custkey\n" +
//                 "\tand l_orderkey = o_orderkey\n" +
//                 "\tand l_suppkey = s_suppkey\n" +
//                 "  and c_nationkey = s_nationkey\n" +
//                 "  and s_nationkey = n_nationkey\n" +
//                 "  and n_regionkey = r_regionkey\n" +
//                 "  and r_name = 'ASIA' \n" +
//                 "  and o_orderdate >= date '1994-01-01' \n" +
//                 "  and o_orderdate < date '1994-01-01' + interval '1' year\n" +
//                 "group by \n" +
//                 "\tn_name\n" +
//                 "order by \n" +
//                 "\trevenue desc;\n";
//
//         String q6 = "select O_ORDERKEY, percentile_approx(O_TOTALPRICE, 0.99) from tpch.orders group by O_ORDERKEY";
//         // explain(q6);
// //        explain(q1);
// //         explain(q5);
//
//         // String q7 = "select O_ORDERKEY, " +
//         //                 "percentile_approx(O_TOTALPRICE, O_CLERK) " +
//         //                 "from tpch.orders group by O_ORDERKEY";
//         // explain(q7);
//
//
//         String q8 = "select  " +
//                         "substr(O_TOTALPRICE, O_CLERK) " +
//                         "from tpch.orders ";
//         explain(q8);
//     }

    private String explain(String sql) throws Exception {
        // logger.info("start to explain sql\n{}", sql);
        String plan = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        // logger.info("output plan\n{}", plan);
        return plan;
    }

    private String[] sqls = {
        "select \n" +
            "    l_returnflag, --返回标志\n" +
            "    l_linestatus, \n" +
            "    sum(l_quantity) as sum_qty, --总的数量\n" +
            "    sum(l_extendedprice) as sum_base_price, --聚集函数操作\n" +
            "    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, \n" +
            "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, \n" +
            "    avg(l_quantity) as avg_qty, \n" +
            "    avg(l_extendedprice) as avg_price, \n" +
            "    avg(l_discount) as avg_disc, \n" +
            "    count(*) as count_order --每个分组所包含的行数\n" +
            "from \n" +
            "    lineitem\n" +
            "where \n" +
            "    l_shipdate <= date'1998-12-01' - interval '90' day --时间段是随机生成的\n" +
            "group by --分组操作\n" +
            "    l_returnflag, \n" +
            "    l_linestatus\n" +
            "order by --排序操作\n" +
            "    l_returnflag, \n" +
            "    l_linestatus;",

        "select\n" +
            "    s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment \n" +
            "from\n" +
            "    part, supplier, partsupp, nation, region \n" +
            "where\n" +
            "    p_partkey = ps_partkey\n" +
            "    and s_suppkey = ps_suppkey\n" +
            "    and p_size = 15 \n" +
            "    and p_type like '%BRASS' \n" +
            "    and s_nationkey = n_nationkey\n" +
            "    and n_regionkey = r_regionkey\n" +
            "    and r_name = 'EUROPE' \n" +
            "    and ps_supplycost = ( \n" +
            "        select\n" +
            "            min(ps_supplycost) \n" +
            "        from\n" +
            "            partsupp, supplier, nation, region \n" +
            "        where\n" +
            "            p_partkey = ps_partkey\n" +
            "            and s_suppkey = ps_suppkey\n" +
            "            and s_nationkey = n_nationkey\n" +
            "            and n_regionkey = r_regionkey\n" +
            "            and r_name = 'EUROPE'\n" +
            "    )\n" +
            "order by \n" +
            "    s_acctbal desc,\n" +
            "    n_name,\n" +
            "    s_name,\n" +
            "    p_partkey;",

        "select\n" +
            "\tl_orderkey,\n" +
            "\tsum(l_extendedprice*(1-l_discount)) as revenue, \n" +
            "\to_orderdate,\n" +
            "\to_shippriority\n" +
            "from\n" +
            "\tcustomer, orders, lineitem \n" +
            "where\n" +
            "\tc_mktsegment = 'MACHINERY' \n" +
            "\tand c_custkey = o_custkey\n" +
            "\tand l_orderkey = o_orderkey\n" +
            "\tand o_orderdate < date '1995-03-02' \n" +
            "\tand l_shipdate > date '1995-03-02'\n" +
            "\tgroup by \n" +
            "\tl_orderkey, \n" +
            "\to_orderdate, \n" +
            "\to_shippriority \n" +
            "order by \n" +
            "\trevenue desc, \n" +
            "\to_orderdate;"

    };

    private List<String> readSqls() throws IOException, URISyntaxException {
        List<String> result = new ArrayList<>();
        for (int i = 1; i <= 22; i++) {
            String fileName = String.format("tpch/q%d.sql", i);
            URI uri = getClass().getClassLoader().getResource(fileName).toURI();
            File file = new File(uri);
            result.add(FileUtils.readFileToString(file, StandardCharsets.UTF_8));
        }
        return result;
    }

    @Test
    public void explainTest() throws Exception {
        /**
         * PLAN FRAGMENT 0
         *  OUTPUT EXPRS:`default_cluster:tpch`.`lineitem`.`L_ORDERKEY` | `default_cluster:tpch`.`lineitem`.`L_PARTKEY` | `default_cluster:tpch`.`lineitem`.`L_SUPPKEY` | `default_cluster:tpch`.`lineitem`.`L_LINENUMBER` | `default_cluster:tpch`.`lineitem`.`L_QUANTITY` | `default_cluster:tpch`.`lineitem`.`L_EXTENDEDPRICE` | `default_cluster:tpch`.`lineitem`.`L_DISCOUNT` | `default_cluster:tpch`.`lineitem`.`L_TAX` | `default_cluster:tpch`.`lineitem`.`L_RETURNFLAG` | `default_cluster:tpch`.`lineitem`.`L_LINESTATUS` | `default_cluster:tpch`.`lineitem`.`L_SHIPDATE` | `default_cluster:tpch`.`lineitem`.`L_COMMITDATE` | `default_cluster:tpch`.`lineitem`.`L_RECEIPTDATE` | `default_cluster:tpch`.`lineitem`.`L_SHIPINSTRUCT` | `default_cluster:tpch`.`lineitem`.`L_SHIPMODE` | `default_cluster:tpch`.`lineitem`.`L_COMMENT`
         *   PARTITION: UNPARTITIONED
         *
         *   RESULT SINK
         *
         *   1:EXCHANGE
         *
         * PLAN FRAGMENT 1
         *  OUTPUT EXPRS:
         *   PARTITION: HASH_PARTITIONED: `default_cluster:tpch`.`lineitem`.`L_ORDERKEY`
         *
         *   STREAM DATA SINK
         *     EXCHANGE ID: 01
         *     UNPARTITIONED
         *
         *   0:OlapScanNode
         *      TABLE: lineitem
         *      PREAGGREGATION: ON
         *      partitions=1/1
         *      rollup: lineitem
         *      tabletRatio=10/10
         *      tabletList=10168,10170,10172,10174,10176,10178,10180,10182,10184,10186
         *      cardinality=0
         *      avgRowSize=208.0
         *      numNodes=1
         */
        connectContext.setDatabase("default_cluster:tpch");
        String sql = "select * from lineitem";
        // String explain = explain(sql);
        // System.out.println("explain result:\n" + explain);


        String plan = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain graph " + sql);
        System.out.println("explain graph result\n" + plan);
    }

    @Test
    public void runSingleTest() throws Exception {
        String fileName = String.format("tpch/q%d.sql", 15);
        URI uri = getClass().getClassLoader().getResource(fileName).toURI();
        File file = new File(uri);
        String sql = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        connectContext.setDatabase("default_cluster:tpch");

        String explain = explain(sql);
        System.out.println("explain result:\n" + explain);
    }

    @Test
    public void explainTimeTest() throws Exception {
        connectContext.setDatabase("default_cluster:tpch");
        List<String> sqls = readSqls();
        List<ExplainResult> results = new ArrayList<>();
        for (int i = 0; i < sqls.size(); i++) {
            long start = System.nanoTime();
            String explainResult = explain(sqls.get(i));
            long time = (System.nanoTime() - start) / 1000 / 1000;
            results.add(new ExplainResult(i + 1, sqls.get(i), explainResult, time));
        }

        for (int i = 0; i < results.size(); i++) {
            ExplainResult result = results.get(i);
            System.out.println(result.toString());
        }
    }

    private static class ExplainResult {
        public final int id;
        public final String explain;
        public final boolean success;
        public final long timeMs;
        public final String sql;

        public ExplainResult(int id, String sql, String explain, long timeMs) {
            this.id = id;
            this.sql = sql;
            this.explain = explain;
            this.timeMs = timeMs;
            success = explain.contains("PLAN FRAGMENT");
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                       .add("id", id)
                       .add("success", success)
                       .add("timeMs", timeMs)
                       .add("sql length", sql.length())
                       .toString();
        }
    }

    // query 1 time: 691 ms
    // query 2 time: 290 ms
    // query 3 time: 134 ms
    // query 4 time: 178 ms
    // query 5 time: 214 ms
    // query 6 time: 55 ms
    //
    //
    // query 1 explain time: 719 ms
    // query 2 explain time: 297 ms
    // query 3 explain time: 174 ms
    // query 4 explain time: 260 ms
    // query 5 explain time: 216 ms
    // query 6 explain time: 55 ms


    //todo: how to disable debug log in UT?
}
