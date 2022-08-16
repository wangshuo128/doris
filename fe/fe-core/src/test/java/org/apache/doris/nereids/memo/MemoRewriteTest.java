package org.apache.doris.nereids.memo;

import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Objects;

public class MemoRewriteTest implements PatternMatchSupported {
    private ConnectContext connectContext = MemoTestUtils.createConnectContext();

    @Test
    public void testRewriteBottomPlanToOnePlan() {
        LogicalOlapScan student = new LogicalOlapScan(PlanConstructor.student);
        LogicalOlapScan score = new LogicalOlapScan(PlanConstructor.score);

        PlanChecker.from(connectContext, student)
                .applyBottomUp(
                        logicalOlapScan().when(scan -> Objects.equals(student, scan)).then(scan -> score)
                )
                .checkGroupNum(1)
                .checkFirstRootLogicalPlan(score)
                .matches(logicalOlapScan().when(score::equals));
    }

    @Test
    public void testRewriteBottomPlanToMultiPlan() {
        LogicalOlapScan student = new LogicalOlapScan(PlanConstructor.student);
        LogicalOlapScan score = new LogicalOlapScan(PlanConstructor.score);
        LogicalLimit<LogicalOlapScan> limit = new LogicalLimit<>(1, 0, score);

        PlanChecker.from(connectContext, student)
                .applyBottomUp(
                        logicalOlapScan().when(scan -> Objects.equals(student, scan)).then(scan -> limit)
                )
                .checkGroupNum(2)
                .checkFirstRootLogicalPlan(limit)
                .matches(
                        logicalLimit(
                                any().when(child -> Objects.equals(child, score))
                        ).when(limit::equals)
                );
    }

    @Test
    public void testRewriteUnboundToBound() {
        UnboundRelation unboundTable = new UnboundRelation(ImmutableList.of("score"));
        LogicalOlapScan boundTable = new LogicalOlapScan(PlanConstructor.score);

        PlanChecker.from(connectContext, unboundTable)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertTrue(logicalProperties instanceof UnboundLogicalProperties);
                })
                .applyBottomUp(unboundRelation().then(unboundRelation -> boundTable))
                .checkGroupNum(1)
                .checkFirstRootLogicalPlan(boundTable)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertEquals(
                            boundTable.getLogicalProperties().getOutput(), logicalProperties.getOutput());
                })
                .matches(
                        logicalOlapScan().when(boundTable::equals)
                );
    }

    @Test
    public void testRewriteTwoLevelUnboundToBound() {
        UnboundRelation unboundTable = new UnboundRelation(ImmutableList.of("score"));
        LogicalLimit<UnboundRelation> unboundLimit = new LogicalLimit<>(1, 0, unboundTable);

        LogicalOlapScan boundTable = new LogicalOlapScan(PlanConstructor.score);
        LogicalLimit<Plan> boundLimit = unboundLimit.withChildren(ImmutableList.of(boundTable));

        PlanChecker.from(connectContext, unboundLimit)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertTrue(logicalProperties instanceof UnboundLogicalProperties);
                })
                .applyBottomUp(unboundRelation().then(unboundRelation -> boundTable))
                .applyBottomUp(
                        logicalPlan()
                                .when(plan -> !plan.resolved()
                                        && !(plan instanceof LeafPlan)
                                        && !(plan instanceof Unbound)
                                        && plan.childrenResolved())
                                .then(LogicalPlan::recomputeLogicalProperties)
                )
                .checkGroupNum(2)
                .checkFirstRootLogicalPlan(boundLimit)
                .checkMemo(memo -> {
                    LogicalProperties logicalProperties = memo.getRoot().getLogicalProperties();
                    Assertions.assertEquals(
                            boundTable.getLogicalProperties().getOutput(), logicalProperties.getOutput());
                })
                .matches(
                        logicalLimit(
                                logicalOlapScan().when(boundTable::equals)
                        ).when(boundLimit::equals)
                );
    }
}
