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

package org.apache.doris.sql.analyzer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.sql.analysis.UnresolvedRelation;
import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.expr.NamedExpression;
import org.apache.doris.sql.plan.logical.Filter;
import org.apache.doris.sql.plan.logical.Join;
import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.sql.plan.logical.Project;
import org.apache.doris.sql.plan.logical.Relation;
import org.apache.doris.sql.plan.logical.SubqueryAlias;
import org.apache.doris.sql.rule.LogicalPlanVisitor;

public class Analyzer {
    public final ConnectContext connectContext;
    private final Catalog catalog;

    private final ExprAnalyzer exprAnalyzer = new ExprAnalyzer(this);

    public Analyzer(ConnectContext connectContext) {
        this.connectContext = connectContext;
        this.catalog = connectContext.getCatalog();
    }

    public LogicalPlan analyze(LogicalPlan plan) {
        return analyze(plan, Optional.empty());
    }

    public LogicalPlan analyze(LogicalPlan plan, Optional<Scope> outerScope) {
        return new QueryAnalyzer(outerScope).visit(plan, null);
    }

    private class QueryAnalyzer extends LogicalPlanVisitor<LogicalPlan, Void> {
        // Outer scope would be set if this is an analyzer for plan in a subquery.
        private final Optional<Scope> outerScope;

        public QueryAnalyzer(Optional<Scope> outerScope) {
            this.outerScope = outerScope;
        }

        @Override
        public LogicalPlan visit(LogicalPlan plan, Void context) {
            return plan.accept(this, context);
        }

        @Override
        public LogicalPlan visitProject(Project project, Void context) {
            LogicalPlan visitedChild = visit(project.child, context);
            List<NamedExpression> output =
                project.projectList.stream()
                    // todo: handle if can't cast to NamedExpression
                    .map(expr -> (NamedExpression) exprAnalyzer.analyze(expr, toScope(visitedChild.getOutput())))
                    .collect(Collectors.toList());
            return new Project(output, visitedChild);
        }

        @Override
        public LogicalPlan visitFilter(Filter filter, Void context) {
            LogicalPlan child = filter.child;
            LogicalPlan visitedChild = visit(child, context);
            Expression analyzedCond = exprAnalyzer.analyze(filter.condition, toScope(visitedChild.getOutput()));
            return new Filter(visitedChild, analyzedCond);
        }

        @Override
        public LogicalPlan visitUnresolvedRelation(UnresolvedRelation plan, Void context) {
            List<String> tableId = plan.getTableId();
            switch (tableId.size()) {
                case 1:
                    return new Relation(getTable(connectContext::getDatabase, () -> tableId.get(0)));
                case 2:
                    return new Relation(getTable(() -> tableId.get(0), () -> tableId.get(1)));
                default:
                    throw new RuntimeException("Invalid tableId: " + StringUtils.join(tableId));
            }
        }

        private Table getTable(Supplier<String> dbSupplier, Supplier<String> tableSupplier) {
            String dbName = dbSupplier.get();
            Database db =
                catalog.getDb(dbName)
                    .orElseThrow(() -> new RuntimeException("Database <" + dbName + "> does not exist."));
            String tableName = tableSupplier.get();
            return db.getTable(tableName).orElseThrow(() -> new RuntimeException(
                "Table " + tableName + " does not exist in " + dbName + "."));
        }

        @Override
        public LogicalPlan visitRelation(Relation relation, Void context) {
            // todo: add a common default delegation?
            return relation;
        }

        @Override
        public LogicalPlan visitJoin(Join join, Void context) {
            LogicalPlan visitLeft = visit(join.left, context);
            LogicalPlan visitRight = visit(join.right, context);
            List<Attribute> output = new ArrayList<>();
            output.addAll(visitLeft.getOutput());
            output.addAll(visitRight.getOutput());
            Optional<Expression> cond =
                join.condition.map(expr -> exprAnalyzer.analyze(expr, toScope(output)));
            return new Join(visitLeft, visitRight, join.type, cond);
        }

        @Override
        public LogicalPlan visitSubqueryAlias(SubqueryAlias plan, Void context) {
            LogicalPlan child = plan.child;
            LogicalPlan visited = visit(child, context);
            if (visited != child) {
                return new SubqueryAlias(plan.aliasName, visited);
            } else {
                return child;
            }
        }

        private Scope toScope(List<Attribute> attrs) {
            if (outerScope.isPresent()) {
                return new Scope(outerScope, attrs);
            } else {
                return Scope.of(attrs);
            }
        }
    }
}
