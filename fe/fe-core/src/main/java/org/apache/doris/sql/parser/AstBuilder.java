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

package org.apache.doris.sql.parser;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.sql.analysis.UnresolvedAlias;
import org.apache.doris.sql.analysis.UnresolvedAttribute;
import org.apache.doris.sql.analysis.UnresolvedRelation;
import org.apache.doris.sql.analysis.UnresolvedStar;
import org.apache.doris.sql.expr.Alias;
import org.apache.doris.sql.expr.BinaryPredicate;
import org.apache.doris.sql.expr.BinaryPredicate.Operator;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.expr.InPredicate;
import org.apache.doris.sql.expr.IntLiteral;
import org.apache.doris.sql.expr.NamedExpression;
import org.apache.doris.sql.expr.SubQuery;
import org.apache.doris.sql.parser.DorisSqlParser.ColumnReferenceContext;
import org.apache.doris.sql.parser.DorisSqlParser.ComparisonContext;
import org.apache.doris.sql.parser.DorisSqlParser.DereferenceContext;
import org.apache.doris.sql.parser.DorisSqlParser.IntegerLiteralContext;
import org.apache.doris.sql.parser.DorisSqlParser.JoinCriteriaContext;
import org.apache.doris.sql.parser.DorisSqlParser.JoinRelationContext;
import org.apache.doris.sql.parser.DorisSqlParser.MultipartIdentifierContext;
import org.apache.doris.sql.parser.DorisSqlParser.NamedExpressionContext;
import org.apache.doris.sql.parser.DorisSqlParser.NamedExpressionSeqContext;
import org.apache.doris.sql.parser.DorisSqlParser.PredicateContext;
import org.apache.doris.sql.parser.DorisSqlParser.PredicatedContext;
import org.apache.doris.sql.parser.DorisSqlParser.QualifiedNameContext;
import org.apache.doris.sql.parser.DorisSqlParser.RelationContext;
import org.apache.doris.sql.parser.DorisSqlParser.StarContext;
import org.apache.doris.sql.parser.DorisSqlParser.StrictIdentifierContext;
import org.apache.doris.sql.parser.DorisSqlParser.TableNameContext;
import org.apache.doris.sql.parser.DorisSqlParser.WhereClauseContext;
import org.apache.doris.sql.plan.logical.Filter;
import org.apache.doris.sql.plan.logical.Join;
import org.apache.doris.sql.plan.logical.LogicalPlan;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.doris.sql.plan.logical.Project;
import org.apache.doris.sql.plan.logical.SubqueryAlias;

public class AstBuilder extends DorisSqlBaseVisitor<Object> {

    private LogicalPlan plan(ParserRuleContext tree) {
        return (LogicalPlan) tree.accept(this);
    }

    private Expression expr(ParserRuleContext tree) {
        return (Expression) tree.accept(this);
    }

    @Override
    public LogicalPlan visitQuery(org.apache.doris.sql.parser.DorisSqlParser.QueryContext ctx) {
        // public LogicalPlan visitQuery(org.apache.doris.sql.parser.DorisSqlParser.QueryContext ctx) {

        LogicalPlan plan = plan(ctx.queryTerm());

        // visit query orders
        return plan;
    }

    @Override
    public LogicalPlan visitRegularQuerySpecification(DorisSqlParser.RegularQuerySpecificationContext ctx) {
        LogicalPlan from = visitFromClause(ctx.fromClause());

        // where ...
        WhereClauseContext whereClause = ctx.whereClause();
        LogicalPlan withFilter;
        if (whereClause == null) {
            withFilter = from;
        } else {
            withFilter = withWhereClause(from, whereClause);
        }

        // select ...
        List<Expression> selectExprs = visitNamedExpressionSeq(ctx.selectClause().namedExpressionSeq());
        LogicalPlan withProject;
        if (!selectExprs.isEmpty()) {
            List<NamedExpression> namedExprs =
                selectExprs.stream().map(expr -> {
                    if (expr instanceof NamedExpression) {
                        return (NamedExpression) expr;
                    } else {
                        return new UnresolvedAlias(expr);
                    }
                }).collect(Collectors.toList());
            withProject = new Project(namedExprs, withFilter);
        } else {
            withProject = withFilter;
        }
        return withProject;
    }

    @Override
    public LogicalPlan visitFromClause(DorisSqlParser.FromClauseContext ctx) {
        List<RelationContext> relations = ctx.relation();
        LogicalPlan last = null;
        for (RelationContext relation : relations) {
            LogicalPlan current = plan(relation.relationPrimary());
            if (last == null) {
                last = current;
            } else {
                last = new Join(last, current, JoinOperator.INNER_JOIN, Optional.empty());
            }
            last = withJoinRelations(last, relation);
        }
        return last;
    }

    private LogicalPlan withWhereClause(LogicalPlan child, WhereClauseContext ctx) {
        return new Filter(child, expr(ctx));
    }

    private LogicalPlan withJoinRelations(LogicalPlan base, RelationContext ctx) {
        List<JoinRelationContext> joinRelations = ctx.joinRelation();
        LogicalPlan last = base;
        for (JoinRelationContext join : joinRelations) {
            JoinOperator joinType;
            if (join.joinType().LEFT() != null) {
                joinType = JoinOperator.LEFT_OUTER_JOIN;
            } else if (join.joinType().RIGHT() != null) {
                joinType = JoinOperator.RIGHT_OUTER_JOIN;
            } else if (join.joinType().FULL() != null) {
                joinType = JoinOperator.FULL_OUTER_JOIN;
            } else {
                joinType = JoinOperator.INNER_JOIN;
            }

            JoinCriteriaContext joinCriteria = join.joinCriteria();
            Optional<Expression> condition;
            if (joinCriteria == null) {
                condition = Optional.empty();
            } else {
                condition = Optional.of(expr(joinCriteria.booleanExpression()));
            }

            last = new Join(last, plan(join.relationPrimary()), joinType, condition);
        }
        return last;
    }

    @Override
    public LogicalPlan visitTableName(TableNameContext ctx) {
        List<String> tableId = visitMultipartIdentifier(ctx.multipartIdentifier());
        UnresolvedRelation relation = new UnresolvedRelation(tableId);
        StrictIdentifierContext strictIdentifier = ctx.tableAlias().strictIdentifier();
        if (strictIdentifier == null) {
            return relation;
        } else {
            return new SubqueryAlias(strictIdentifier.getText(), relation);
        }
    }

    @Override
    public List<String> visitMultipartIdentifier(MultipartIdentifierContext ctx) {
        return ctx.parts.stream().map(RuleContext::getText).collect(Collectors.toList());
    }

    @Override
    public Expression visitColumnReference(ColumnReferenceContext ctx) {
        // todo: handle quoted and unquoted
        return new UnresolvedAttribute(ctx.getText());
    }

    @Override
    public Expression visitDereference(DereferenceContext ctx) {
        String filedName = ctx.fieldName.getText();
        // todo: base is an expression, may be not a table name.
        return new UnresolvedAttribute(ctx.base.getText(), filedName);
    }

    @Override
    public Expression visitComparison(ComparisonContext ctx) {
        Expression left = expr(ctx.left);
        Expression right = expr(ctx.right);
        TerminalNode op = (TerminalNode) ctx.comparisonOperator().getChild(0);
        switch (op.getSymbol().getType()) {
            case DorisSqlParser.EQ:
                return new BinaryPredicate(left, right, Operator.EQ);
            case DorisSqlParser.NEQJ:
                return new BinaryPredicate(left, right, Operator.NE);
            case DorisSqlParser.LT:
                return new BinaryPredicate(left, right, Operator.LT);
            case DorisSqlParser.LTE:
                return new BinaryPredicate(left, right, Operator.LE);
            case DorisSqlParser.GT:
                return new BinaryPredicate(left, right, Operator.GT);
            case DorisSqlParser.GTE:
                return new BinaryPredicate(left, right, Operator.GE);
            default:
                throw new RuntimeException("not support comparison operator: " + op.getSymbol().getType());
        }
    }

    @Override
    public Object visitIntegerLiteral(IntegerLiteralContext ctx) {
        long l = Long.parseLong(ctx.getText());
        return new IntLiteral(l);
    }

    @Override
    public Expression visitNamedExpression(NamedExpressionContext ctx) {
        Expression expr = expr(ctx.expression());
        if (ctx.name != null) {
            return new Alias(expr, ctx.name.getText());
        } else {
            return expr;
        }
    }

    @Override
    public List<Expression> visitNamedExpressionSeq(NamedExpressionSeqContext ctx) {
        return ctx.namedExpression().stream()
                   .map(this::visitNamedExpression).collect(Collectors.toList());
    }

    @Override
    public Object visitStar(StarContext ctx) {
        QualifiedNameContext qualifiedNameContext = ctx.qualifiedName();
        if (qualifiedNameContext != null) {
            List<String> qualifiedName = ctx.qualifiedName().identifier()
                                             .stream().map(RuleContext::getText)
                                             .collect(Collectors.toList());
            return new UnresolvedStar(Optional.of(qualifiedName));
        } else {
            return new UnresolvedStar(Optional.empty());
        }
    }

    @Override
    public Expression visitPredicated(PredicatedContext ctx) {
        Expression expr = expr(ctx.valueExpression());
        if (ctx.predicate() != null) {
            return withPredicate(expr, ctx.predicate());
        } else {
            return expr;
        }
    }

    /**
     * Add a predicate to the given expression. Supported expressions are:
     * - (NOT) BETWEEN
     * - (NOT) IN
     * - (NOT) (LIKE | ILIKE) (ANY | SOME | ALL)
     * - (NOT) RLIKE
     * - IS (NOT) NULL.
     * - IS (NOT) (TRUE | FALSE | UNKNOWN)
     * - IS (NOT) DISTINCT FROM
     */
    private Expression withPredicate(Expression e, PredicateContext ctx) {
        switch (ctx.kind.getType()) {
            case DorisSqlParser.IN:
                if (ctx.query() == null) {
                    // in subquery
                    // todo: handle not in, etc.
                    return new InPredicate(e, new SubQuery(plan(ctx.query())));
                } else {
                    // in value list
                    throw new RuntimeException("not support yet");
                }
            default:
                throw new RuntimeException("not supported yet.");
        }
    }
}
