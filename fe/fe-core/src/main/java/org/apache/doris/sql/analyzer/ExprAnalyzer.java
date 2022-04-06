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

package org.apache.doris.sql.analyzer;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.doris.sql.analysis.UnresolvedAttribute;
import org.apache.doris.sql.analysis.UnresolvedStar;
import org.apache.doris.sql.expr.Alias;
import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.expr.AttributeReference;
import org.apache.doris.sql.expr.BinaryPredicate;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.expr.IntLiteral;
import org.apache.doris.sql.rule.ExprVisitor;

public class ExprAnalyzer {

    private List<Attribute> refAttrs;

    public Expression analyze(Expression expr, List<Attribute> attrs) {
        this.refAttrs = attrs;
        Visitor visitor = new Visitor();
        return visitor.visit(expr, null);
    }

    private class Visitor extends ExprVisitor<Expression, Void> {

        @Override
        public Expression visit(Expression expr, Void context) {
            return expr.accept(this, context);
        }

        @Override
        public Expression visitUnresolvedAttribute(UnresolvedAttribute expr, Void context) {
            List<Attribute> resolved = ResolveUtils.resolveAttr(expr, refAttrs);
            switch (resolved.size()) {
                case 0:
                    throw new RuntimeException("Cannot resolve " + expr.toString());
                case 1:
                    return resolved.get(0);
                default:
                    throw new RuntimeException(expr + " is ambiguous： " +
                                                   resolved.stream().map(attr -> attr.toString())
                                                       .collect(Collectors.joining(", ")));
            }
        }

        @Override
        public Expression visitUnresolvedStar(UnresolvedStar expr, Void context) {
            // todo: impl
            return super.visitUnresolvedStar(expr, context);
        }

        @Override
        public Expression visitAlias(Alias expr, Void context) {
            Expression newChild = visit(expr.child, context);
            if (newChild == expr.child) {
                return expr;
            } else {
                return new Alias(newChild, expr.name);
            }
        }

        @Override
        public Expression visitAttributeReference(AttributeReference attr, Void context) {
            return attr;
        }

        @Override
        public Expression visitBinaryPredicate(BinaryPredicate bp, Void context) {
            Expression newLeft = visit(bp.left, context);
            Expression newRight = visit(bp.right, context);
            if (newLeft != bp.left || newRight != bp.right) {
                return new BinaryPredicate(newLeft, newRight, bp.op);
            } else {
                return bp;
            }
        }

        @Override
        public Expression visitIntLiteral(IntLiteral expr, Void context) {
            return expr;
        }
    }
}
