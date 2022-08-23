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
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rules to push {@link org.apache.doris.nereids.trees.plans.logical.LogicalLimit} down.
 */
public class LimitPushDown implements RewriteRuleFactory {


    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.PUSH_LIMIT_THROUGH_JOIN.build(
                logicalLimit(logicalJoin()).then(limit -> {
                    LogicalJoin<GroupPlan, GroupPlan> join = limit.child();
                    switch (join.getJoinType()) {
                        case LEFT_OUTER_JOIN:
                            return join.withChildren(limit.withChildren(join.left()), join.right());
                        case RIGHT_OUTER_JOIN:
                            return join.withChildren(join.left(), limit.withChildren(join.right()));
                        case CROSS_JOIN:
                            return join.withChildren(limit.withChildren(join.left()), limit.withChildren(join.right()));
                        case INNER_JOIN:
                            if (!join.getCondition().isPresent()) {
                                return join.withChildren(
                                    limit.withChildren(join.left()),
                                    limit.withChildren(join.right())
                                );
                            } else {
                                return limit;
                            }
                        case LEFT_ANTI_JOIN:
                            // todo: support anti join.
                        default:
                            return limit;
                    }
                })
            )
        );
    }
}
