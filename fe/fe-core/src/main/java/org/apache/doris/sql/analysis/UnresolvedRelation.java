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

package org.apache.doris.sql.analysis;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.sql.rule.LogicalPlanVisitor;

public class UnresolvedRelation extends LogicalPlan {

    private final List<String> tableId;

    public UnresolvedRelation(List<String> tableId) {
        this.tableId = tableId;
    }

    public List<String> getTableId() {
        return tableId;
    }

    @Override
    public List<LogicalPlan> getChildren() {
        return Lists.newArrayList();
    }

    @Override
    public List<Attribute> getOutput() {
        return Collections.emptyList();
    }

    @Override
    public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnresolvedRelation(this, context);
    }

    @Override
    public String toString() {
        return new StringBuilder().append("UnresolvedRelation").append("(")
                   .append(StringUtils.join(tableId, ", "))
                   .append(")").toString();
    }
}
