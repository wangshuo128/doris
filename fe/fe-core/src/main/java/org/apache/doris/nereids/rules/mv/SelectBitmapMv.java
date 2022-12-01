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

package org.apache.doris.nereids.rules.mv;

/**
 * todo: xxx
 */
public class SelectBitmapMv{
    //
    // @Override
    // public Rule build() {
    //     return logicalAggregate(logicalProject(logicalOlapScan())).then(agg -> {
    //         LogicalProject<LogicalOlapScan> project = agg.child();
    //         LogicalOlapScan scan = project.child();
    //         OlapTable table = scan.getTable();
    //         List<Count> counts = agg.getOutputExpressions()
    //                 .stream()
    //                 .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance).stream())
    //                 .filter(aggFun -> {
    //                     if (aggFun instanceof Count) {
    //                         Count count = (Count) aggFun;
    //                         return count.isDistinct();
    //                     } else {
    //                         return false;
    //                     }
    //                 })
    //                 .map(Count.class::cast)
    //                 .collect(Collectors.toList());
    //
    //         System.out.println("counts size is " + counts.size());
    //         if (counts.size() != 1) {
    //             return agg;
    //         }
    //
    //         Count count = counts.get(0);
    //         Slot countChild = (SlotReference) count.child(0);
    //         String childName = countChild.getName();
    //         String mvColumnName = CreateMaterializedViewStmt
    //                 .mvColumnBuilder(AggregateType.BITMAP_UNION.name().toLowerCase(), childName);
    //
    //         Column mvColumn = table.getVisibleColumn(mvColumnName);
    //         if (mvColumn == null) {
    //             System.out.println("mv column is null");
    //             return agg;
    //         } else {
    //             System.out.println("mv column is not null");
    //             // new scan
    //             List<Long> indexIds = table.getVisibleIndex()
    //                     .stream()
    //                     .filter(index -> index.getId() != table.getBaseIndexId())
    //                     .map(MaterializedIndex::getId)
    //                     .collect(Collectors.toList());
    //             // LogicalOlapScan newScan = scan.withMaterializedIndexSelected(PreAggStatus.on(), indexIds);
    //
    //             // new project
    //             Slot slot = scan.getOutput()
    //                     .stream()
    //                     .filter(s -> s.getName().equals(mvColumnName))
    //                     .findFirst().get();
    //             BitmapUnionCount bitmapUnionCount = new BitmapUnionCount(slot);
    //             ImmutableMap<Expression, Expression> replaceMap = ImmutableMap.of(countChild, slot);
    //             ImmutableList<NamedExpression> newProjects = project.getProjects()
    //                     .stream()
    //                     .map(expr -> ExpressionUtils.replace(expr, replaceMap))
    //                     .map(NamedExpression.class::cast)
    //                     .collect(ImmutableList.toImmutableList());
    //             // LogicalProject<LogicalOlapScan> newProject = new LogicalProject<>(newProjects, newScan);
    //
    //             // new agg
    //             ImmutableMap<Expression, Expression> aggReplaceMap = ImmutableMap.of(count, bitmapUnionCount);
    //             List<NamedExpression> newAggOutput = agg.getOutputExpressions()
    //                     .stream()
    //                     .map(expr -> ExpressionUtils.replace(expr, aggReplaceMap))
    //                     .map(NamedExpression.class::cast)
    //                     .collect(Collectors.toList());
    //
    //             // return new LogicalAggregate<>(agg.getGroupByExpressions(), newAggOutput, newProject);
    //             return null;
    //         }
    //
    //     }).toRule(RuleType.MV_BITMAP);
    // }
}
