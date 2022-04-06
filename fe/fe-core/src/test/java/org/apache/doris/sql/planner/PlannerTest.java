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

package org.apache.doris.sql.planner;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.common.FeConstants;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanPrinter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.sql.analyzer.Analyzer;
import org.apache.doris.sql.optimizer.PruneColumn;
import org.apache.doris.sql.optimizer.EliminateProject;
import org.apache.doris.sql.optimizer.Optimizer;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PlannerTest {
    private static String runningDir;
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        runningDir = "fe/mocked/PlannerTest/" + UUID.randomUUID().toString() + "/";
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
                        "distributed by hash(k2) buckets 3\n" +
                        "properties('replication_num' = '1');";

        String t1 = "create table t1(\n" +
                        "id int, \n" +
                        "k1 int, \n" +
                        "k2 int, \n" +
                        "k3 int, \n" +
                        "v1 int, \n" +
                        "v2 int)\n" +
                        "distributed by hash(k2) buckets 1\n" +
                        "properties('replication_num' = '1');";

        String t2 = "create table t2(\n" +
                        "id int, \n" +
                        "k1 int, \n" +
                        "k2 int, \n" +
                        "k3 int, \n" +
                        "v1 int, \n" +
                        "v2 int)\n" +
                        "distributed by hash(k2) buckets 1\n" +
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
        SqlParser parser = new SqlParser();
        // LogicalPlan parsedPlan = parser.parse(sql1);
        LogicalPlan parsedPlan = parser.parse(sql4);
        System.out.println("====parsed plan:====\n" + parsedPlan.treeString() + "\n");
        Analyzer analyzer = new Analyzer(connectContext);
        LogicalPlan analyzed = analyzer.analyze(parsedPlan);
        System.out.println("====analyzed plan:====\n" + analyzed.treeString() + "\n");
        Optimizer optimizer = Optimizer.create(PruneColumn.INSTANCE, EliminateProject.INSTANCE);
        LogicalPlan optimized = optimizer.optimize(analyzed);
        System.out.println("====optimized plan:====\n" + optimized.treeString() + "\n");
        DescriptorTable descTable = new DescriptorTable();
        PlannerContext plannerContext = new PlannerContext(descTable);
        Planner planner = new Planner();
        List<PlanFragment> planFragments = planner.planToFragments(optimized, plannerContext);
        String explain =
            PlanPrinter.explain(planFragments, new ExplainOptions(true, false), Optional.of(descTable));
        System.out.println("====plan fragment====\n" + explain);
    }
}