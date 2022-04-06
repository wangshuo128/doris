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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanPrinter;
import org.apache.doris.sql.analyzer.Analyzer;
import org.apache.doris.sql.optimizer.PruneColumn;
import org.apache.doris.sql.optimizer.EliminateProject;
import org.apache.doris.sql.optimizer.Optimizer;
import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.sql.planner.Planner;
import org.apache.doris.sql.planner.PlannerContext;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SqlExecutor {
    private static final Logger LOG = LogManager.getLogger(SqlExecutor.class);

    private final String sql;
    private final ConnectContext context;

    public SqlExecutor(String sql, ConnectContext context) {
        this.sql = sql;
        this.context = context;
    }

    public void dryRun() throws Exception {
        doExecute(false);
    }

    public void execute() throws Exception {
        doExecute(true);
    }

    private void doExecute(boolean sendFragments) throws Exception {
        LOG.info("==== input SQL: ====\n{}", sql);
        System.out.println("==== input SQL: ====\n" + sql + "\n");

        // parse phase
        org.apache.doris.sql.parser.SqlParser parser = new org.apache.doris.sql.parser.SqlParser();
        LogicalPlan parsedPlan = parser.parse(sql);
        LOG.info("==== parsed plan: ====\n{}", parsedPlan.treeString());
        System.out.println("==== parsed plan: ====\n" + parsedPlan.treeString() + "\n");

        // analyze phase
        Analyzer analyzer = new Analyzer(context);
        LogicalPlan analyzed = analyzer.analyze(parsedPlan);
        LOG.info("==== analyzed plan: ====\n{}", analyzed.treeString());
        System.out.println("==== analyzed plan: ====\n" + analyzed.treeString() + "\n");

        // optimize phase
        Optimizer optimizer = Optimizer.create(PruneColumn.INSTANCE, EliminateProject.INSTANCE);
        LogicalPlan optimized = optimizer.optimize(analyzed);
        LOG.info("==== optimized plan: ====\n{}", optimized.treeString());
        System.out.println("==== optimized plan: ====\n" + optimized.treeString() + "\n");

        // planning phase: generate physical plan (PlanNode) and executable plan fragments
        DescriptorTable descTable = new DescriptorTable();
        PlannerContext plannerContext = new PlannerContext(descTable);
        Planner planner = new Planner();
        List<PlanFragment> planFragments = planner.planToFragments(optimized, plannerContext);

        String explainResult = PlanPrinter.explain(planFragments,
            new ExplainOptions(true, true),
            Optional.of(descTable));

        LOG.info("==== executable fragments: ====\n{}", explainResult);
        System.out.println("==== executable fragments: ====\n" + explainResult);

        // set query id
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        context.setQueryId(queryId);

        // run fragments in coordinator
        RowBatch batch;
        MysqlChannel channel = context.getMysqlChannel();
        Coordinator coordinator = new Coordinator(planFragments, context, descTable);
        QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
            new QeProcessorImpl.QueryInfo(context, sql, coordinator));
        coordinator.exec(sendFragments);
        while (true) {
            batch = coordinator.getNext();
            if (batch.getBatch() != null) {
                // todo: send fields
                for (ByteBuffer row : batch.getBatch().getRows()) {
                    channel.sendOnePacket(row);
                }
            }
            if (batch.isEos()) {
                break;
            }
        }
    }
}
