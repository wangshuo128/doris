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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.catalog.Table;
import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.expr.AttributeReference;
import org.apache.doris.sql.rule.LogicalPlanVisitor;

public class Relation extends LogicalPlan {
    public final Table table;
    private final List<Attribute> output;

    public Relation(Table table) {
        this.table = table;
        output = table.getBaseSchema()
                     .stream()
                     .map(col -> AttributeReference.fromColumn(col, table.getName()))
                     .collect(Collectors.toList());
    }

    private Relation(Table table, List<Attribute> output) {
        this.table = table;
        this.output = output;
    }

    @Override
    public List<Attribute> getOutput() {
        return output;
    }

    @Override
    public List<LogicalPlan> getChildren() {
        return new ArrayList<>();
    }

    @Override
    public String toString() {
        return new StringBuilder("Relation(")
                   .append(table.getName())
                   .append(", output: ")
                   .append(StringUtils.join(output, ", "))
                   .append(")").toString();
    }

    public Relation withNewOutput(List<Attribute> output) {
        return new Relation(table, output);
    }

    @Override
    public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) {
        return visitor.visitRelation(this, context);
    }
}
