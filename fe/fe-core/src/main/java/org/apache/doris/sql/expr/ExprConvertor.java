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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.sql.rule.ExprVisitor;

public class ExprConvertor {

    private final DescriptorTable descTable;

    public ExprConvertor(DescriptorTable descTable) {
        this.descTable = descTable;
    }

    public Expr convert(Expression expr) {
        return new Visitor().visit(expr, null);
    }

    private class Visitor extends ExprVisitor<Expr, Void> {

        @Override
        public Expr visit(Expression expr, Void context) {
            return expr.accept(this, context);
        }

        @Override
        public Expr visitAlias(Alias expr, Void context) {
            return super.visitAlias(expr, context);
        }

        @Override
        public Expr visitAttributeReference(AttributeReference attr, Void context) {
            SlotRef slotRef = new SlotRef(descTable.getSlotDescByAttrId(attr.getExprId()));
            // todo: handle nullable, see `PlanNode.nullableTupleIds`
            slotRef.setType(attr.getType());
            return slotRef;
        }

        @Override
        public Expr visitBinaryPredicate(BinaryPredicate bp, Void context) {
            Expr left = visit(bp.left, context);
            Expr right = visit(bp.right, context);

            Operator op;
            switch (bp.op) {
                case EQ:
                    op = Operator.EQ;
                    break;
                case GE:
                    op = Operator.GE;
                    break;
                case LE:
                    op = Operator.LE;
                    break;
                case LT:
                    op = Operator.LT;
                    break;
                case GT:
                    op = Operator.GT;
                    break;
                default:
                    throw new RuntimeException("not supported op: " + bp.op);
            }

            return new org.apache.doris.analysis.BinaryPredicate(op, left, right);
        }

        @Override
        public Expr visitIntLiteral(IntLiteral expr, Void context) {
            return new org.apache.doris.analysis.IntLiteral(expr.value);
        }
    }

    public static List<Expr> toOldVersion(List<Expression> exprs, DescriptorTable descTable) {
        ExprConvertor convertor = new ExprConvertor(descTable);
        return exprs.stream().map(convertor::convert).collect(Collectors.toList());
    }
}
