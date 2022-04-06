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

package org.apache.doris.sql.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.collections.ListUtils;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.sql.analysis.UnresolvedRelation;
import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.expr.AttributeSet;
import org.apache.doris.sql.expr.BinaryPredicate;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.plan.logical.Filter;
import org.apache.doris.sql.plan.logical.Join;
import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.sql.plan.logical.Project;
import org.apache.doris.sql.plan.logical.Relation;
import org.apache.doris.sql.rule.LogicalPlanVisitor;

import static org.apache.doris.sql.expr.ExprConvertor.toOldVersion;

/**
 * Convert a LogicalPlan to Physical Plan (PlanNode).
 */
public class PhysicalPlanner {

    private final PlannerContext ctx;
    private final DescriptorTable descTable;

    public PhysicalPlanner(PlannerContext ctx) {
        this.ctx = ctx;
        this.descTable = ctx.descTable;
    }

    public PlanNode convert(LogicalPlan plan) {
        return new Visitor().visit(plan, null);
    }

    private class Visitor extends LogicalPlanVisitor<PlanNode, Void> {
        @Override
        public PlanNode visit(LogicalPlan plan, Void context) {
            return plan.accept(this, context);
        }

        @Override
        public PlanNode visitProject(Project project, Void context) {
            return super.visitProject(project, context);
        }

        @Override
        public PlanNode visitFilter(Filter filter, Void context) {
            return super.visitFilter(filter, context);
        }

        @Override
        public PlanNode visitUnresolvedRelation(UnresolvedRelation plan, Void context) {
            return super.visitUnresolvedRelation(plan, context);
        }

        @Override
        public PlanNode visitRelation(Relation relation, Void context) {
            // todo: current only support OlapScanNode
            TupleDescriptor tupleDesc = descTable.createTupleDescriptor();

            // add table ref
            Table table = relation.table;
            tupleDesc.setTable(table);
            TableRef tableRef = new TableRef();
            BaseTableRef baseTableRef = new BaseTableRef(tableRef, table, new TableName("", table.getName()));
            tupleDesc.setRef(baseTableRef);

            // add slot desc
            Map<String, Column> nameToColumn =
                table.getBaseSchema()
                    .stream()
                    .collect(Collectors.toMap(Column::getName, Function.identity()));

            for (Attribute attr : relation.getOutput()) {
                Optional<Column> column = Optional.ofNullable(nameToColumn.get(attr.getName()));
                if (column.isPresent()) {
                    Column column1 = column.get();
                    SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
                    slotDesc.setColumn(column1);
                    slotDesc.setIsMaterialized(true);
                    slotDesc.setIsNullable(column1.isAllowNull());

                    descTable.registerAttrId(attr.getExprId(), slotDesc.getId());
                }
            }

            // todo: maybe we should perform this logic later...
            descTable.computeStatAndMemLayout();
            OlapScanNode scanNode = new OlapScanNode(ctx.getNextNodeId(), tupleDesc, "OlapScanNode");
            try {
                scanNode.completeState();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return scanNode;
        }

        @Override
        public PlanNode visitJoin(Join join, Void context) {
            PlanNode left = visit(join.left, context);
            PlanNode right = visit(join.right, context);

            // only support hash join currently
            if (!join.condition.isPresent()) {
                throw new RuntimeException("Cross join is not supported");
            }
            Expression expression = join.condition.get();
            // List<Expression> conjuncts = ExprUtils.splitConjuncts(expression);
            List<Expression> conjuncts = Lists.newArrayList(expression);
            List<Expression> equalToConjuncts = extractEqualJoinCondition(join.left, join.right, conjuncts);
            List<Expression> otherConjuncts = ListUtils.removeAll(conjuncts, equalToConjuncts);

            HashJoinNode hashJoin = new HashJoinNode(ctx.getNextNodeId(), left, right, join.type,
                toOldVersion(equalToConjuncts, descTable),
                toOldVersion(otherConjuncts, descTable));
            return hashJoin;
        }

        // todo: refine this logic
        private List<Expression> extractEqualJoinCondition(LogicalPlan left,
                                                           LogicalPlan right,
                                                           List<Expression> conjuncts) {
            AttributeSet leftOutput = new AttributeSet(left.getOutput());
            AttributeSet rightOutput = new AttributeSet(right.getOutput());
            List<Expression> result = new ArrayList<>();
            for (Expression conjunct : conjuncts) {
                if (conjunct instanceof BinaryPredicate) {
                    BinaryPredicate bc = (BinaryPredicate) conjunct;
                    if ((leftOutput.contains(bc.left.getReferences()) &&
                             rightOutput.contains(bc.right.getReferences())) ||
                            (leftOutput.contains(bc.right.getReferences()) &&
                                 rightOutput.contains(bc.left.getReferences()))) {
                        result.add(bc);
                    }
                }
            }
            return result;
        }

    }
}
