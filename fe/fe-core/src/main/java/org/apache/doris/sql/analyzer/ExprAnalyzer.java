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
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.doris.sql.analysis.UnresolvedAttribute;
import org.apache.doris.sql.analysis.UnresolvedStar;
import org.apache.doris.sql.expr.Alias;
import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.expr.AttributeReference;
import org.apache.doris.sql.expr.BinaryPredicate;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.expr.InPredicate;
import org.apache.doris.sql.expr.IntLiteral;
import org.apache.doris.sql.expr.SubQuery;
import org.apache.doris.sql.plan.logical.LogicalPlan;
import org.apache.doris.sql.rule.ExprVisitor;

public class ExprAnalyzer {

    private final Analyzer analyzer;

    public ExprAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public Expression analyze(Expression expr, Scope scope) {
        Visitor visitor = new Visitor(scope);
        return visitor.visit(expr, null);
    }

    private class Visitor extends ExprVisitor<Expression, Void> {
        private final Scope scope;

        public Visitor(Scope scope) {
            this.scope = scope;
        }

        @Override
        public Expression visit(Expression expr, Void context) {
            return expr.accept(this, context);
        }

        @Override
        public Expression visitUnresolvedAttribute(UnresolvedAttribute expr, Void context) {
            List<Attribute> resolved = scope.resolve(expr);
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

        @Override
        public Expression visitInPredicate(InPredicate in, Void context) {
            Expression newValue = visit(in.value, context);
            Expression newElements = visit(in.elements, context);
            return new InPredicate(newValue, newElements);
        }

        @Override
        public Expression visitSubQuery(SubQuery subQuery, Void context) {
            Analyzer subqueryAnalyzer = new Analyzer(ExprAnalyzer.this.analyzer.connectContext);
            // Try to analyze subquery in a new analyzer, with current scope as outer scope
            // to find references in.
            LogicalPlan analyzed = subqueryAnalyzer.analyze(subQuery.plan, Optional.of(scope));
            return new SubQuery(analyzed);
        }
    }
}
