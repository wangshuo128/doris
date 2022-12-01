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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rewrite count distinct for bitmap and hll type.
 *
 * count(distinct bitmap_col) -> bitmap_union_count(bitmap col)
 * count(distinct hll_col) ->
 */
public class CountDistinctRewrite extends OneRewriteRuleFactory {
    private static final CountDistinctRewriter REWRITER = new CountDistinctRewriter();

    @Override
    public Rule build() {
        return logicalAggregate().then(agg -> {
            List<NamedExpression> output = agg.getOutputExpressions()
                    .stream()
                    .map(REWRITER::rewrite)
                    .map(NamedExpression.class::cast)
                    .collect(ImmutableList.toImmutableList());
            return new LogicalAggregate<>(agg.getGroupByExpressions(), output,
                    agg.isDisassembled(), agg.isNormalized(),
                    agg.isFinalPhase(), agg.getAggPhase(), agg.child());
        }).toRule(RuleType.COUNT_DISTINCT_REWRITE);
    }

    private static class CountDistinctRewriter extends DefaultExpressionRewriter<Void> {
        public Expression rewrite(Expression expr) {
            return expr.accept(this, null);
        }

        @Override
        public Expression visitCount(Count count, Void context) {
            if (count.isStar()) {
                return count;
            }

            Expression child = count.child(0).accept(this, null);
            if (child.getDataType().isBitmap()) {
                return new BitmapUnionCount(child);
            } else {
                return count.withChildren(child);
            }
        }
    }
}
