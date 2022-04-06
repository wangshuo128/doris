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
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeVisitor;

/**
 * Convert PlanNode to PlanFragments
 */
public class DistributePlanner {

    private final PlannerContext ctx;

    public DistributePlanner(PlannerContext ctx) {
        this.ctx = ctx;
    }

    public List<PlanFragment> planToFragments(PlanNode plan) {
        List<PlanFragment> fragments = new ArrayList<>();
        PlanFragment fragmentRoot = new Visitor().visit(plan, fragments);

        if (fragmentRoot.isPartitioned() && fragmentRoot.getPlanRoot().getNumInstances() > 1) {
            fragmentRoot = createMergeFragment(fragmentRoot);
            fragments.add(fragmentRoot);
        }

        for (PlanFragment f : fragments) {
            f.finalize(null);
        }

        Collections.reverse(fragments);
        return fragments;
    }

    /**
     * Return unpartitioned fragment that merges the input fragment's output via
     * an ExchangeNode.
     * Requires that input fragment be partitioned.
     */
    private PlanFragment createMergeFragment(PlanFragment inputFragment) {
        Preconditions.checkState(inputFragment.isPartitioned());

        // exchange node clones the behavior of its input, aside from the conjuncts
        ExchangeNode mergePlan =
            new ExchangeNode(ctx.getNextNodeId(), inputFragment.getPlanRoot(), false);
        mergePlan.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
        // mergePlan.init(ctx_.getRootAnalyzer());
        // Preconditions.checkState(mergePlan.hasValidStats());
        PlanFragment fragment = new PlanFragment(ctx.getNextFragmentId(),
            mergePlan, DataPartition.UNPARTITIONED);
        inputFragment.setDestination(mergePlan);
        return fragment;
    }


    private class Visitor extends PlanNodeVisitor<PlanFragment, List<PlanFragment>> {

        @Override
        public PlanFragment visit(PlanNode plan, List<PlanFragment> context) {
            return plan.accept(this, context);
        }

        @Override
        public PlanFragment visitOlapScanNode(OlapScanNode scan, List<PlanFragment> context) {
            try {
                PlanFragment f = new PlanFragment(ctx.getNextFragmentId(), scan,
                    scan.constructInputPartitionByDistributionInfo(), DataPartition.RANDOM);
                context.add(f);
                return f;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public PlanFragment visitHashJoinNode(HashJoinNode join, List<PlanFragment> context) {
            PlanFragment right = visit(join.getChild(1), context);
            PlanFragment left = visit(join.getChild(0), context);

            // only support broadcast join for POC
            join.setDistributionMode(DistributionMode.BROADCAST);
            join.setChild(0, left.getPlanRoot());
            connectChildFragment(join, 1, left, right);
            left.setPlanRoot(join);
            return left;
        }

        @Override
        public PlanFragment visitExchangeNode(ExchangeNode exchange, List<PlanFragment> context) {
            return super.visitExchangeNode(exchange, context);
        }

        /**
         * Replace node's child at index childIdx with an ExchangeNode that receives its input
         * from childFragment.
         */
        private void connectChildFragment(PlanNode node,
                                          int childIdx,
                                          PlanFragment parentFragment,
                                          PlanFragment childFragment) {
            ExchangeNode exchangeNode =
                new ExchangeNode(ctx.getNextNodeId(), childFragment.getPlanRoot(), false);
            exchangeNode.setNumInstances(childFragment.getPlanRoot().getNumInstances());
            // exchangeNode.init(ctx.getRootAnalyzer());
            exchangeNode.setFragment(parentFragment);
            node.setChild(childIdx, exchangeNode);
            childFragment.setDestination(exchangeNode);
        }
    }
}
