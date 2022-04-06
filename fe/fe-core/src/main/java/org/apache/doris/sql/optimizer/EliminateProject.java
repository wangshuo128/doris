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

import org.apache.doris.sql.expr.AttributeSet;
import org.apache.doris.sql.plan.logical.Filter;
import org.apache.doris.sql.plan.logical.Join;
import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.sql.plan.logical.Project;
import org.apache.doris.sql.plan.logical.Relation;
import org.apache.doris.sql.plan.logical.Sink;
import org.apache.doris.sql.plan.logical.SubqueryAlias;
import org.apache.doris.sql.rule.LogicalPlanVisitor;

public class EliminateProject extends VisitorBasedRule {

    public static EliminateProject INSTANCE = new EliminateProject();

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return new Visitor().visit(plan, null);
    }

    private class Visitor extends LogicalPlanVisitor<LogicalPlan, Void> {
        @Override
        public LogicalPlan visit(LogicalPlan plan, Void context) {
            return plan.accept(this, null);
        }

        @Override
        public LogicalPlan visitProject(Project project, Void context) {
            if (isIdentityProject(project, project.child)) {
                return visit(project.child, context);
            } else {
                LogicalPlan newChild = visit(project.child, context);
                if (newChild == project.child) {
                    return project;
                } else {
                    return new Project(project.projectList, newChild);
                }
            }
        }

        @Override
        public LogicalPlan visitFilter(Filter filter, Void context) {
            return visit(filter.child, context);
        }

        @Override
        public LogicalPlan visitRelation(Relation relation, Void context) {
            return relation;
        }

        @Override
        public LogicalPlan visitJoin(Join join, Void context) {
            LogicalPlan newLeft = visit(join.left, context);
            LogicalPlan newRight = visit(join.right, context);
            return new Join(newLeft, newRight, join.type, join.condition, join.getOutput());
        }

        @Override
        public LogicalPlan visitSubqueryAlias(SubqueryAlias plan, Void context) {
            return super.visitSubqueryAlias(plan, context);
        }

        @Override
        public LogicalPlan visitSink(Sink sink, Void context) {
            return super.visitSink(sink, context);
        }

        private boolean isIdentityProject(Project project, LogicalPlan plan) {
            return new AttributeSet(project.getOutput()).equals(new AttributeSet(plan.getOutput()));
        }
    }
}
