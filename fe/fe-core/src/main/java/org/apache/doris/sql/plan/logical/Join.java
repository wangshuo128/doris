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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.rule.LogicalPlanVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Join extends LogicalPlan {
    public final LogicalPlan left;
    public final LogicalPlan right;
    public final JoinOperator type;
    public final Optional<Expression> condition;
    private final List<Attribute> output;

    public Join(LogicalPlan left, LogicalPlan right, JoinOperator type, Optional<Expression> condition) {
        this.left = left;
        this.right = right;
        this.type = type;
        this.condition = condition;
        this.output = new ArrayList<>();
        output.addAll(left.getOutput());
        output.addAll(right.getOutput());
    }

    public Join(LogicalPlan left, LogicalPlan right, JoinOperator type,
                Optional<Expression> condition,
                List<Attribute> output) {
        this.left = left;
        this.right = right;
        this.type = type;
        this.condition = condition;
        this.output = output;
        // todo: check that output must be subset of left output + right output.
    }

    public static Join of(LogicalPlan left, LogicalPlan right, JoinOperator type, Expression condition) {
        if (condition == null) {
            return new Join(left, right, type, Optional.empty());
        } else {
            return new Join(left, right, type, Optional.of(condition));
        }
    }

    // todo: refine this with respect to join type
    @Override
    public List<Attribute> getOutput() {
        return output;
    }

    @Override
    public List<LogicalPlan> getChildren() {
        return Lists.newArrayList(left, right);
    }

    @Override
    public List<Expression> getExpressions() {
        return condition.map(Lists::newArrayList).orElseGet(ArrayList::new);
    }

    @Override
    public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) {
        return visitor.visitJoin(this, context);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Join(").append(type);
        condition.ifPresent(expression -> sb.append(", ").append(expression.toString()));
        sb.append(", output: ").append(StringUtils.join(output, ", "));
        return sb.append(")").toString();
    }
}
