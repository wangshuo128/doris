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

package org.apache.doris.sql.expr;

import java.util.ArrayList;
import java.util.List;

import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.sql.rule.ExprVisitor;

public class SubQuery extends Expression {
    public final LogicalPlan plan;

    public SubQuery(LogicalPlan plan) {
        this.plan = plan;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitSubQuery(this, context);
    }

    @Override
    public String toString() {
        return "SubQuery";
    }

    @Override
    public List<Expression> getChildren() {
        // todo: impl
        return new ArrayList<>();
    }

    public LogicalPlan getPlan() {
        return plan;
    }
}
