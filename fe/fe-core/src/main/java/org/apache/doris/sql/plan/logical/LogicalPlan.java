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

package org.apache.doris.sql.plan.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.expr.AttributeSet;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.expr.SubQuery;
import org.apache.doris.sql.rule.LogicalPlanVisitor;
import org.apache.doris.sql.tree.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LogicalPlan extends TreeNode<LogicalPlan> {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public abstract List<Attribute> getOutput();

    public List<? extends Expression> getExpressions() {
        return Collections.emptyList();
    }

    public AttributeSet getReferences() {
        List<AttributeSet> sets =
            getExpressions()
                .stream()
                .map(Expression::getReferences)
                .collect(Collectors.toList());
        return AttributeSet.fromAttributeSets(sets);
    }

    public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) {
        System.out.println("accept plan, visitor: "
                               + visitor.getClass().getName() +
                               ", plan: " + this.getClass().getSimpleName());
        return visitor.visit(this, context);
    }

    public List<LogicalPlan> getSubQueryPlans() {
        List<SubQuery> subQueries = new ArrayList<>();
        getExpressions().forEach(expr -> {
            expr.collectAll(Predicates.instanceOf(SubQuery.class), subQueries);
        });
        return subQueries.stream().map(SubQuery::getPlan).collect(Collectors.toList());
    }

    public String treeString() {
        StringBuilder builder = new StringBuilder();
        treeString(builder, 0, new ArrayList<>(), this);
        return builder.toString();
    }

    private void treeString(StringBuilder builder, int depth,
                            List<Boolean> lastChildren, LogicalPlan plan) {
        if (depth > 0) {
            // grand parents of the current node
            if (lastChildren.size() > 1) {
                for (int i = 0; i < lastChildren.size() - 1; i++) {
                    builder.append(lastChildren.get(i) ? "    " : "│   ");
                }
            }
            // direct parent of the current node
            if (lastChildren.size() > 0) {
                Boolean last = lastChildren.get(lastChildren.size() - 1);
                builder.append(last ? "└──" : "├──");
            }
        }
        builder.append(plan);
        builder.append("\n");

        List<LogicalPlan> children = plan.getChildren();

        List<LogicalPlan> subQueryPlans = plan.getSubQueryPlans();
        for (int i = 0; i < subQueryPlans.size(); i++) {
            List<Boolean> newLasts = new ArrayList<>(lastChildren);
            newLasts.add(children.isEmpty());
            newLasts.add(i + 1 == subQueryPlans.size());
            treeString(builder, depth + 1, newLasts, subQueryPlans.get(i));
        }

        for (int i = 0; i < children.size(); i++) {
            List<Boolean> newLasts = new ArrayList<>(lastChildren);
            newLasts.add(i + 1 == children.size());
            treeString(builder, depth + 1, newLasts, children.get(i));
        }
    }
}
