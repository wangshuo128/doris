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

import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.plan.logical.Filter;
import org.apache.doris.sql.plan.logical.Join;
import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.sql.rule.LogicalPlanVisitor;

public class PredicatePushDown extends VisitorBasedRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return null;
    }

    private class Visitor extends LogicalPlanVisitor<LogicalPlan, List<Expression>> {
        @Override
        public LogicalPlan visit(LogicalPlan plan, List<Expression> context) {
            return plan.accept(this, context);
        }

        @Override
        public LogicalPlan visitFilter(Filter filter, List<Expression> context) {
            // todo
            return null;
        }

        @Override
        public LogicalPlan visitJoin(Join join, List<Expression> context) {
            // todo
            return null;
        }
    }
}
