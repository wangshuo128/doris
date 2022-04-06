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

package org.apache.doris.sql.optimizer;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.doris.sql.analysis.UnresolvedRelation;
import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.expr.AttributeSet;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.expr.NamedExpression;
import org.apache.doris.sql.plan.logical.Filter;
import org.apache.doris.sql.plan.logical.Join;
import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.sql.plan.logical.Project;
import org.apache.doris.sql.plan.logical.Relation;
import org.apache.doris.sql.plan.logical.SubqueryAlias;
import org.apache.doris.sql.rule.LogicalPlanVisitor;

public class PruneColumn extends VisitorBasedRule {

    public static PruneColumn INSTANCE = new PruneColumn();

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        List<Attribute> output = plan.getOutput();
        return new Visitor().visit(plan, new AttributeSet(output));
    }

    private class Visitor extends LogicalPlanVisitor<LogicalPlan, AttributeSet> {

        @Override
        public LogicalPlan visit(LogicalPlan plan, AttributeSet context) {
            return plan.accept(this, context);
        }

        @Override
        public LogicalPlan visitProject(Project project, AttributeSet context) {
            List<NamedExpression> prunedProjectList =
                project.projectList.stream()
                    .filter(expr -> context.contains(expr.toAttribute()))
                    .collect(Collectors.toList());

            List<AttributeSet> required =
                prunedProjectList.stream().map(Expression::getReferences).collect(Collectors.toList());
            LogicalPlan newChild = project.child.accept(this, AttributeSet.fromAttributeSets(required));
            return new Project(prunedProjectList, newChild);
        }

        @Override
        public LogicalPlan visitFilter(Filter filter, AttributeSet context) {
            AttributeSet required = AttributeSet.fromAttributeSets(
                context, filter.condition.getReferences());
            LogicalPlan newChild = visit(filter.child, required);
            return new Filter(newChild, filter.condition);
        }

        @Override
        public LogicalPlan visitUnresolvedRelation(UnresolvedRelation plan, AttributeSet context) {
            throw new RuntimeException("not supported");
        }

        @Override
        public LogicalPlan visitRelation(Relation relation, AttributeSet context) {
            List<Attribute> pruned =
                relation.getOutput().stream().filter(context::contains)
                    .collect(Collectors.toList());
            return relation.withNewOutput(pruned);
        }

        @Override
        public LogicalPlan visitJoin(Join join, AttributeSet context) {
            Optional<AttributeSet> condAttrs = join.condition.map(Expression::getReferences);

            List<Attribute> prunedOutput =
                join.getOutput()
                    .stream()
                    .filter(context::contains).collect(Collectors.toList());

            AttributeSet prunedOutputSet = new AttributeSet(prunedOutput);
            AttributeSet required =
                condAttrs.map(attrs -> AttributeSet.fromAttributeSets(attrs, prunedOutputSet))
                    .orElse(prunedOutputSet);

            LogicalPlan newLeft = visit(join.left, required);
            LogicalPlan newRight = visit(join.right, required);
            return new Join(newLeft, newRight, join.type, join.condition, prunedOutput);
        }

        @Override
        public LogicalPlan visitSubqueryAlias(SubqueryAlias plan, AttributeSet context) {
            throw new RuntimeException("to be supported...");
        }
    }
}
