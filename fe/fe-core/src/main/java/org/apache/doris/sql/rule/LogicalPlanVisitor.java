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

package org.apache.doris.sql.rule;

import org.apache.doris.sql.analysis.UnresolvedRelation;
import org.apache.doris.sql.plan.logical.Filter;
import org.apache.doris.sql.plan.logical.Join;
import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.sql.plan.logical.Project;
import org.apache.doris.sql.plan.logical.Relation;
import org.apache.doris.sql.plan.logical.Sink;
import org.apache.doris.sql.plan.logical.SubqueryAlias;

public abstract class LogicalPlanVisitor<R, C> {

    // todo: support both logical plan and physical plan?
    public abstract R visit(LogicalPlan plan, C context);

    public R visitProject(Project project, C context) {
        return visit(project, context);
    }

    public R visitFilter(Filter filter, C context) {
        return visit(filter, context);
    }

    public R visitUnresolvedRelation(UnresolvedRelation plan, C context) {
        return visit(plan, context);
    }

    public R visitRelation(Relation relation, C context) {
        return visit(relation, context);
    }

    public R visitJoin(Join join, C context) {
        return visit(join, context);
    }

    public R visitSubqueryAlias(SubqueryAlias plan, C context) {
        return visit(plan, context);
    }

    public R visitSink(Sink sink, C context) {
        return visit(sink, context);
    }
}
