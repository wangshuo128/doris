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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.rule.LogicalPlanVisitor;

public class SubqueryAlias extends LogicalPlan {
    public String aliasName;
    public LogicalPlan child;
    private List<Attribute> output = null;

    public SubqueryAlias(String aliasName, LogicalPlan child) {
        this.aliasName = aliasName;
        this.child = child;
    }

    @Override
    public List<Attribute> getOutput() {
        if (output == null) {
            this.output = child.getOutput().stream().map(a -> a.withNewQualifier(Optional.of(aliasName))).collect(Collectors.toList());
        }
        return output;
    }

    @Override
    public List<LogicalPlan> getChildren() {
        return Lists.newArrayList(child);
    }

    @Override
    public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) {
        return visitor.visitSubqueryAlias(this, context);
    }

    @Override
    public String toString() {
        return new StringBuilder("SubqueryAlias(").append(aliasName).append(")").toString();
    }
}
