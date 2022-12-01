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

import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Select materialized index, i.e., both for rollup and materialized view when aggregate is present.
 * TODO: optimize queries with aggregate not on top of scan directly, e.g., aggregate -> join -> scan
 *   to use materialized index.
 */
@Developing
public class SelectMaterializedIndexWithAggregate extends AbstractSelectMaterializedIndexRule
        implements RewriteRuleFactory {
    ///////////////////////////////////////////////////////////////////////////
    // All the patterns
    ///////////////////////////////////////////////////////////////////////////
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // only agg above scan
                // Aggregate(Scan)
                logicalAggregate(logicalOlapScan().when(this::shouldSelectIndex)).then(agg -> {
                    LogicalOlapScan scan = agg.child();
                    SelectResult result = select(
                            scan,
                            agg.getInputSlots(),
                            ImmutableList.of(),
                            extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                            agg.getGroupByExpressions());
                    if (result.slotMap.isEmpty()) {
                        return agg.withChildren(
                                scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                        );
                    } else {
                        return new LogicalAggregate<>(
                                agg.getGroupByExpressions(),
                                replaceAggOutput(agg, Optional.empty(), result.aggFuncMap),
                                agg.isDisassembled(),
                                agg.isNormalized(),
                                agg.isFinalPhase(),
                                agg.getAggPhase(),
                                agg.getSourceRepeat(),
                                scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                        );
                    }
                }).toRule(RuleType.MATERIALIZED_INDEX_AGG_SCAN),

                // filter could push down scan.
                // Aggregate(Filter(Scan))
                logicalAggregate(logicalFilter(logicalOlapScan().when(this::shouldSelectIndex)))
                        .then(agg -> {
                            LogicalFilter<LogicalOlapScan> filter = agg.child();
                            LogicalOlapScan scan = filter.child();
                            ImmutableSet<Slot> requiredSlots = ImmutableSet.<Slot>builder()
                                    .addAll(agg.getInputSlots())
                                    .addAll(filter.getInputSlots())
                                    .build();

                            SelectResult result =
                                    select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.empty()),
                                    agg.getGroupByExpressions()
                            );

                            if (result.slotMap.isEmpty()) {
                                return agg.withChildren(filter.withChildren(
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                                ));
                            } else {
                                return new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.empty(), result.aggFuncMap),
                                        agg.isDisassembled(),
                                        agg.isNormalized(),
                                        agg.isFinalPhase(),
                                        agg.getAggPhase(),
                                        agg.getSourceRepeat(),
                                        filter.withChildren(
                                                scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId))
                                );
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_FILTER_SCAN),

                // column pruning or other projections such as alias, etc.
                // Aggregate(Project(Scan))
                logicalAggregate(logicalProject(logicalOlapScan().when(this::shouldSelectIndex)))
                        .then(agg -> {
                            System.out.println("select mv index");
                            LogicalProject<LogicalOlapScan> project = agg.child();
                            LogicalOlapScan scan = project.child();
                            SelectResult result = select(
                                    scan,
                                    project.getInputSlots(),
                                    ImmutableList.of(),
                                    extractAggFunctionAndReplaceSlot(agg,
                                            Optional.of(project)),
                                    agg.getGroupByExpressions()
                            );

                            // todo: replace slot when have project
                            // add test case:
                            // select k1, sum(v) from (
                            //         select k1, v1 + 1 as v from t
                            // ) t group by k1
                            if (result.slotMap.isEmpty()) {
                                return agg.withChildren(
                                        project.withChildren(
                                                scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                                        )
                                );
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project, result.slotMap);
                                LogicalProject<LogicalOlapScan> newProject = new LogicalProject<>(
                                        newProjectList,
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId));
                                LogicalAggregate<LogicalProject<LogicalOlapScan>> resultAgg = new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.of(project), result.aggFuncMap),
                                        agg.isDisassembled(),
                                        agg.isNormalized(),
                                        agg.isFinalPhase(),
                                        agg.getAggPhase(),
                                        agg.getSourceRepeat(),
                                        newProject
                                );
                                System.out.println("result: " + resultAgg.treeString());
                                return resultAgg;
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_PROJECT_SCAN),

                // filter could push down and project.
                // Aggregate(Project(Filter(Scan)))
                logicalAggregate(logicalProject(logicalFilter(logicalOlapScan()
                        .when(this::shouldSelectIndex)))).then(agg -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = agg.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            Set<Slot> requiredSlots = Stream.concat(
                                    project.getInputSlots().stream(), filter.getInputSlots().stream())
                                    .collect(Collectors.toSet());
                            SelectResult result = select(
                                    scan,
                                    requiredSlots,
                                    filter.getConjuncts(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer())
                            );

                            if (result.slotMap.isEmpty()) {
                                return agg.withChildren(project.withChildren(filter.withChildren(
                                        scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                                )));
                            } else {
                                List<NamedExpression> newProjectList = replaceProjectList(project, result.slotMap);

                                LogicalProject<Plan> newProject = new LogicalProject<>(newProjectList,
                                        filter.withChildren(scan.withMaterializedIndexSelected(result.preAggStatus,
                                                result.indexId)));

                                return new LogicalAggregate<>(
                                        agg.getGroupByExpressions(),
                                        replaceAggOutput(agg, Optional.of(project), result.aggFuncMap),
                                        agg.isDisassembled(),
                                        agg.isNormalized(),
                                        agg.isFinalPhase(),
                                        agg.getAggPhase(),
                                        agg.getSourceRepeat(),
                                        newProject
                                );
                            }
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_PROJECT_FILTER_SCAN),

                // filter can't push down
                // Aggregate(Filter(Project(Scan)))
                logicalAggregate(logicalFilter(logicalProject(logicalOlapScan()
                        .when(this::shouldSelectIndex)))).then(agg -> {
                            LogicalFilter<LogicalProject<LogicalOlapScan>> filter = agg.child();
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();
                            SelectResult result = select(
                                    scan,
                                    project.getInputSlots(),
                                    ImmutableList.of(),
                                    extractAggFunctionAndReplaceSlot(agg, Optional.of(project)),
                                    ExpressionUtils.replace(agg.getGroupByExpressions(),
                                            project.getAliasToProducer())
                            );
                            return agg.withChildren(filter.withChildren(project.withChildren(
                                    scan.withMaterializedIndexSelected(result.preAggStatus, result.indexId)
                            )));
                        }).toRule(RuleType.MATERIALIZED_INDEX_AGG_FILTER_PROJECT_SCAN)
        );
    }

    ///////////////////////////////////////////////////////////////////////////
    // Main entrance of select materialized index.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Select materialized index ids.
     * <p>
     * 1. find candidate indexes by pre-agg status: checking input aggregate functions and group by expressions
     * and pushdown predicates.
     * 2. filter and order the candidate indexes.
     */
    private SelectResult select(
            LogicalOlapScan scan,
            Set<Slot> requiredScanOutput,
            List<Expression> predicates,
            List<AggregateFunction> aggregateFunctions,
            List<Expression> groupingExprs) {
        Preconditions.checkArgument(scan.getOutputSet().containsAll(requiredScanOutput),
                String.format("Scan's output (%s) should contains all the input required scan output (%s).",
                        scan.getOutput(), requiredScanOutput));

        OlapTable table = scan.getTable();

        // 0. check pre-aggregation status.
        switch (table.getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS: {
                // Check pre-aggregation status by base index for aggregate-keys and unique-keys OLAP table.
                PreAggStatus preAggStatus = checkPreAggStatus(scan, table.getBaseIndexId(), predicates,
                        aggregateFunctions, groupingExprs);
                if (preAggStatus.isOff()) {
                    // return early if pre agg status if off.
                    return new SelectResult(preAggStatus, scan.getTable().getBaseIndexId(), ImmutableMap.of(),
                            ImmutableMap.of());
                } else {
                    List<Long> sortedIndexId = filterAndOrder(table.getVisibleIndex().stream(), scan, predicates);
                    return new SelectResult(preAggStatus,
                            CollectionUtils.isEmpty(sortedIndexId)
                                    ? scan.getTable().getBaseIndexId() : sortedIndexId.get(0),
                            ImmutableMap.of(), ImmutableMap.of());
                }
            }
            case DUP_KEYS: {
                Map<Boolean, List<MaterializedIndex>> indexesGroupByIsBaseOrNot = table.getVisibleIndex()
                        .stream()
                        .collect(Collectors.groupingBy(index -> index.getId() == table.getBaseIndexId()));

                System.out.println("agg funcs: " + aggregateFunctions);

                // Duplicate-keys table could use base index and indexes that pre-aggregation status is on.
                Stream<MaterializedIndex> checkPreAggResult = Stream.concat(
                        indexesGroupByIsBaseOrNot.get(true).stream(),
                        indexesGroupByIsBaseOrNot.getOrDefault(false, ImmutableList.of())
                                .stream()
                                .filter(index -> checkPreAggStatus(scan, index.getId(), predicates,
                                        aggregateFunctions, groupingExprs).isOn())
                );

                // try to rewrite bitmap, hll by materialized index columns.
                Set<MaterializedIndex> collect = checkPreAggResult.collect(Collectors.toSet());
                List<AggRewriteResult> couldRewrite = indexesGroupByIsBaseOrNot.getOrDefault(false, ImmutableList.of())
                        .stream()
                        .filter(index -> !collect.contains(index))
                        .map(index -> rewriteAgg(index, scan, requiredScanOutput, predicates, aggregateFunctions,
                                groupingExprs))
                        .filter(result -> result.success)
                        .collect(Collectors.toList());

                List<MaterializedIndex> haveAllRequiredColumns = Streams.concat(
                        collect.stream()
                                .filter(index -> containAllRequiredColumns(index, scan, requiredScanOutput)),
                        couldRewrite
                                .stream()
                                .filter(aggRewriteResult -> containAllRequiredColumns(aggRewriteResult.index, scan,
                                        aggRewriteResult.requiredScanOutput))
                                .map(aggRewriteResult -> aggRewriteResult.index)
                ).collect(Collectors.toList());

                List<Long> sortedIndexId = filterAndOrder(haveAllRequiredColumns.stream(), scan, predicates);

                long selectIndexId = CollectionUtils.isEmpty(sortedIndexId)
                        ? scan.getTable().getBaseIndexId() : sortedIndexId.get(0);
                Optional<AggRewriteResult> rewriteResultOpt = couldRewrite.stream()
                        .filter(aggRewriteResult -> aggRewriteResult.index.getId() == selectIndexId)
                        .findAny();
                System.out.println("sorted index ids: " + sortedIndexId + ", select index id: " + selectIndexId);
                // Pre-aggregation is set to `on` by default for duplicate-keys table.
                return new SelectResult(PreAggStatus.on(), selectIndexId,
                        rewriteResultOpt.map(r -> r.slotMap).orElse(ImmutableMap.of()),
                        rewriteResultOpt.map(r -> r.aggFuncMap).orElse(ImmutableMap.of()));
            }
            default:
                throw new RuntimeException("Not supported keys type: " + table.getKeysType());
        }
    }

    private static class SelectResult {
        public final PreAggStatus preAggStatus;
        public final long indexId;
        public final Map<Slot, Slot> slotMap;
        public final Map<AggregateFunction, AggregateFunction> aggFuncMap;

        public SelectResult(PreAggStatus preAggStatus, long indexId, Map<Slot, Slot> slotMap,
                Map<AggregateFunction, AggregateFunction> aggFuncMap) {
            this.preAggStatus = preAggStatus;
            this.indexId = indexId;
            this.slotMap = slotMap;
            this.aggFuncMap = aggFuncMap;
        }
    }

    private static class IndexIdAndSlotMap {
        private final long index;
        private final Map<Slot, Slot> slotMap;

        public IndexIdAndSlotMap(long index, Map<Slot, Slot> slotMap) {
            this.index = index;
            this.slotMap = slotMap;
        }
    }

    /**
     * Do aggregate function extraction and replace aggregate function's input slots by underlying project.
     * <p>
     * 1. extract aggregate functions in aggregate plan.
     * <p>
     * 2. replace aggregate function's input slot by underlying project expression if project is present.
     * <p>
     * For example:
     * <pre>
     * input arguments:
     * agg: Aggregate(sum(v) as sum_value)
     * underlying project: Project(a + b as v)
     *
     * output:
     * sum(a + b)
     * </pre>
     */
    private List<AggregateFunction> extractAggFunctionAndReplaceSlot(
            LogicalAggregate<?> agg,
            Optional<LogicalProject<?>> project) {
        Optional<Map<Slot, Expression>> slotToProducerOpt = project.map(Project::getAliasToProducer);
        return agg.getOutputExpressions().stream()
                // extract aggregate functions.
                .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance).stream())
                // replace aggregate function's input slot by its producing expression.
                .map(expr -> slotToProducerOpt.map(slotToExpressions
                                -> (AggregateFunction) ExpressionUtils.replace(expr, slotToExpressions))
                        .orElse(expr)
                )
                .collect(Collectors.toList());
    }

    ///////////////////////////////////////////////////////////////////////////
    // Set pre-aggregation status.
    ///////////////////////////////////////////////////////////////////////////
    private PreAggStatus checkPreAggStatus(
            LogicalOlapScan olapScan,
            long indexId,
            List<Expression> predicates,
            List<AggregateFunction> aggregateFuncs,
            List<Expression> groupingExprs) {
        CheckContext checkContext = new CheckContext(olapScan, indexId);
        return checkAggregateFunctions(aggregateFuncs, checkContext)
                .offOrElse(() -> checkGroupingExprs(groupingExprs, checkContext))
                .offOrElse(() -> checkPredicates(predicates, checkContext));
    }

    /**
     * Check pre agg status according to aggregate functions.
     */
    private PreAggStatus checkAggregateFunctions(
            List<AggregateFunction> aggregateFuncs,
            CheckContext checkContext) {
        return aggregateFuncs.stream()
                .map(f -> AggregateFunctionChecker.INSTANCE.check(f, checkContext))
                .filter(PreAggStatus::isOff)
                .findAny()
                .orElse(PreAggStatus.on());
    }

    // TODO: support all the aggregate function types in storage engine.
    private static class AggregateFunctionChecker extends ExpressionVisitor<PreAggStatus, CheckContext> {

        public static final AggregateFunctionChecker INSTANCE = new AggregateFunctionChecker();

        public PreAggStatus check(AggregateFunction aggFun, CheckContext ctx) {
            return aggFun.accept(INSTANCE, ctx);
        }

        @Override
        public PreAggStatus visit(Expression expr, CheckContext context) {
            return PreAggStatus.off(String.format("%s is not aggregate function.", expr.toSql()));
        }

        @Override
        public PreAggStatus visitAggregateFunction(AggregateFunction aggregateFunction, CheckContext context) {
            return PreAggStatus.off(String.format("Aggregate %s function is not supported in storage engine.",
                    aggregateFunction.getName()));
        }

        @Override
        public PreAggStatus visitMax(Max max, CheckContext context) {
            return checkAggFunc(max, AggregateType.MAX, extractSlotId(max.child()), context, true);
        }

        @Override
        public PreAggStatus visitMin(Min min, CheckContext context) {
            return checkAggFunc(min, AggregateType.MIN, extractSlotId(min.child()), context, true);
        }

        @Override
        public PreAggStatus visitSum(Sum sum, CheckContext context) {
            return checkAggFunc(sum, AggregateType.SUM, extractSlotId(sum.child()), context, false);
        }

        // TODO: select count(xxx) for duplicated-keys table.
        @Override
        public PreAggStatus visitCount(Count count, CheckContext context) {
            // Now count(distinct key_column) is only supported for aggregate-keys and unique-keys OLAP table.
            if (count.isDistinct()) {
                Optional<ExprId> exprIdOpt = extractSlotId(count.child(0));
                if (exprIdOpt.isPresent() && context.exprIdToKeyColumn.containsKey(exprIdOpt.get())) {
                    return PreAggStatus.on();
                }
            }
            return PreAggStatus.off(String.format(
                    "Count distinct is only valid for key columns, but meet %s.", count.toSql()));
        }

        @Override
        public PreAggStatus visitBitmapUnionCount(BitmapUnionCount bitmapUnionCount, CheckContext context) {
            // todo: some necessary check?
            return PreAggStatus.on();
        }

        private PreAggStatus checkAggFunc(
                AggregateFunction aggFunc,
                AggregateType matchingAggType,
                Optional<ExprId> exprIdOpt,
                CheckContext ctx,
                boolean canUseKeyColumn) {
            return exprIdOpt.map(exprId -> {
                if (ctx.exprIdToKeyColumn.containsKey(exprId)) {
                    if (canUseKeyColumn) {
                        return PreAggStatus.on();
                    } else {
                        Column column = ctx.exprIdToKeyColumn.get(exprId);
                        return PreAggStatus.off(String.format("Aggregate function %s contains key column %s.",
                                aggFunc.toSql(), column.getName()));
                    }
                } else if (ctx.exprIdToValueColumn.containsKey(exprId)) {
                    AggregateType aggType = ctx.exprIdToValueColumn.get(exprId).getAggregationType();
                    if (aggType == matchingAggType) {
                        return PreAggStatus.on();
                    } else {
                        return PreAggStatus.off(String.format("Aggregate operator don't match, aggregate function: %s"
                                + ", column aggregate type: %s", aggFunc.toSql(), aggType));
                    }
                } else {
                    return PreAggStatus.off(String.format("Slot(%s) in %s is neither key column nor value column.",
                            exprId, aggFunc.toSql()));
                }
            }).orElse(PreAggStatus.off(String.format("Input of aggregate function %s should be slot or cast on slot.",
                    aggFunc.toSql())));
        }

        // TODO: support more type of expressions, such as case when.
        private Optional<ExprId> extractSlotId(Expression expr) {
            return ExpressionUtils.isSlotOrCastOnSlot(expr);
        }
    }

    private static class CheckContext {
        public final Map<ExprId, Column> exprIdToKeyColumn;

        // todo: replace bitmap, hll columns.
        public final Map<ExprId, Column> exprIdToValueColumn;

        public final LogicalOlapScan scan;

        public CheckContext(LogicalOlapScan scan, long indexId) {
            this.scan = scan;
            // map<is_key, map<column_name, column>>
            Map<Boolean, Map<String, Column>> nameToColumnGroupingByIsKey
                    = scan.getTable().getSchemaByIndexId(indexId)
                    .stream()
                    .collect(Collectors.groupingBy(
                            Column::isKey,
                            Collectors.toMap(Column::getName, Function.identity())
                    ));
            Map<String, Column> keyNameToColumn = nameToColumnGroupingByIsKey.get(true);
            Map<String, Column> valueNameToColumn = nameToColumnGroupingByIsKey.getOrDefault(false, ImmutableMap.of());
            Map<String, ExprId> nameToExprId = scan.getOutput()
                    .stream()
                    .collect(Collectors.toMap(
                            NamedExpression::getName,
                            NamedExpression::getExprId)
                    );
            this.exprIdToKeyColumn = keyNameToColumn.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> nameToExprId.get(e.getKey()),
                            Entry::getValue)
                    );
            this.exprIdToValueColumn = valueNameToColumn.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> nameToExprId.get(e.getKey()),
                            Entry::getValue)
                    );
            System.out.println("exprIdToValueColumn=" + exprIdToValueColumn);
        }
    }

    /**
     * Grouping expressions should not have value type columns.
     */
    private PreAggStatus checkGroupingExprs(
            List<Expression> groupingExprs,
            CheckContext checkContext) {
        return disablePreAggIfContainsAnyValueColumn(groupingExprs, checkContext,
                "Grouping expression %s contains value column %s");
    }

    /**
     * Predicates should not have value type columns.
     */
    private PreAggStatus checkPredicates(
            List<Expression> predicates,
            CheckContext checkContext) {
        return disablePreAggIfContainsAnyValueColumn(predicates, checkContext,
                "Predicate %s contains value column %s");
    }

    /**
     * Check the input expressions have no referenced slot to underlying value type column.
     */
    private PreAggStatus disablePreAggIfContainsAnyValueColumn(List<Expression> exprs, CheckContext ctx,
            String errorMsg) {
        Map<ExprId, Column> exprIdToValueColumn = ctx.exprIdToValueColumn;
        return exprs.stream()
                .map(expr -> expr.getInputSlots()
                        .stream()
                        .filter(slot -> exprIdToValueColumn.containsKey(slot.getExprId()))
                        .findAny()
                        .map(slot -> Pair.of(expr, exprIdToValueColumn.get(slot.getExprId())))
                )
                .filter(Optional::isPresent)
                .findAny()
                .orElse(Optional.empty())
                .map(exprToColumn -> PreAggStatus.off(String.format(errorMsg,
                        exprToColumn.key().toSql(), exprToColumn.value().getName())))
                .orElse(PreAggStatus.on());
    }

    /**
     * rewrite for bitmap and hll
     */
    private AggRewriteResult rewriteAgg(MaterializedIndex index,
            LogicalOlapScan scan,
            Set<Slot> requiredScanOutput,
            List<Expression> predicates,
            List<AggregateFunction> aggregateFunctions,
            List<Expression> groupingExprs) {
        Map<Slot, Slot> slotMap = Maps.newHashMap();
        Map<AggregateFunction, AggregateFunction> aggFuncMap = Maps.newHashMap();
        RewriteContext context = new RewriteContext(new CheckContext(scan, index.getId()), slotMap, aggFuncMap);
        List<AggregateFunction> rewrittenAggFuncs = aggregateFunctions.stream()
                .map(aggFunc -> (AggregateFunction) AggFuncRewriter.INSTANCE.rewrite(aggFunc, context))
                .collect(ImmutableList.toImmutableList());

        // has rewritten agg functions
        if (!slotMap.isEmpty()) {
            Set<Slot> slotsToReplace = slotMap.keySet();
            if (!isInputSlotsContainsAny(predicates, slotsToReplace)
                    && !isInputSlotsContainsAny(groupingExprs, slotsToReplace)) {
                ImmutableSet<Slot> newRequiredSlots = requiredScanOutput.stream()
                        .map(slot -> (Slot) ExpressionUtils.replace(slot, slotMap))
                        .collect(ImmutableSet.toImmutableSet());
                return new AggRewriteResult(index, true, newRequiredSlots, rewrittenAggFuncs, slotMap,
                        aggFuncMap);
            }
        }

        return new AggRewriteResult(index, false, null, null, null, null);
    }

    private static class AggRewriteResult {
        public final MaterializedIndex index;
        public final boolean success;
        public final Set<Slot> requiredScanOutput;
        public final List<AggregateFunction> aggFuncs;
        public final Map<Slot, Slot> slotMap;
        public final Map<AggregateFunction, AggregateFunction> aggFuncMap;

        public AggRewriteResult(
                MaterializedIndex index,
                boolean success,
                Set<Slot> requiredScanOutput,
                List<AggregateFunction> aggFuncs,
                Map<Slot, Slot> slotMap,
                Map<AggregateFunction, AggregateFunction> aggFuncMap) {
            this.index = index;
            this.success = success;
            this.requiredScanOutput = requiredScanOutput;
            this.aggFuncs = aggFuncs;
            this.slotMap = slotMap;
            this.aggFuncMap = aggFuncMap;
        }
    }

    private boolean isInputSlotsContainsAny(List<Expression> expressions, Set<Slot> slotsToCheck) {
        Set<Slot> inputSlotSet = ExpressionUtils.getInputSlotSet(expressions);
        return ExpressionUtils.isIntersecting(inputSlotSet, slotsToCheck);
    }

    private static class RewriteContext {
        public final CheckContext checkContext;
        public final Map<Slot, Slot> slotMap;
        public final Map<AggregateFunction, AggregateFunction> aggFuncMap;

        public RewriteContext(CheckContext context, Map<Slot, Slot> slotMap,
                Map<AggregateFunction, AggregateFunction> aggFuncMap) {
            this.checkContext = context;
            this.slotMap = slotMap;
            this.aggFuncMap = aggFuncMap;
        }
    }

    private static class AggFuncRewriter extends DefaultExpressionRewriter<RewriteContext> {
        public static final AggFuncRewriter INSTANCE = new AggFuncRewriter();

        private Expression rewrite(Expression expr, RewriteContext context) {
            return expr.accept(this, context);
        }

        @Override
        public Expression visitCount(Count count, RewriteContext context) {
            if (count.isDistinct()) {
                Optional<Slot> slotOpt = ExpressionUtils.extractSlotOnCastOnSlot(count.child(0));

                // count distinct a value column.
                if (slotOpt.isPresent() && !context.checkContext.exprIdToKeyColumn.containsKey(
                        slotOpt.get().getExprId())) {
                    String bitmapUnionCountColumn = CreateMaterializedViewStmt
                            .mvColumnBuilder(AggregateType.BITMAP_UNION.name().toLowerCase(), slotOpt.get().getName());

                    Column mvColumn = context.checkContext.scan.getTable().getVisibleColumn(bitmapUnionCountColumn);
                    // has bitmap_union_count column
                    if (mvColumn != null && context.checkContext.exprIdToValueColumn.containsValue(mvColumn)) {
                        Slot bitmapUnionCountSlot = context.checkContext.scan.getOutput()
                                .stream()
                                .filter(s -> s.getName().equals(bitmapUnionCountColumn))
                                .findFirst()
                                .get();

                        context.slotMap.put(slotOpt.get(), bitmapUnionCountSlot);
                        BitmapUnionCount bitmapUnionCount = new BitmapUnionCount(bitmapUnionCountSlot);
                        context.aggFuncMap.put(count, bitmapUnionCount);
                        return bitmapUnionCount;
                    }
                }
            }
            return count;
        }
    }

    /**
     * <pre>
     * Original input SQL:
     * select k1, count(distinct v) as cnt from (
     *   select k1, v1 as v from t
     * ) t group by k1
     *
     * We have materialized index for table t:
     * mv_bitmap_union_v1 for column v1
     *
     * Before rewriting, the input plan is:
     * Aggregate(count(distinct v) as cnt)
     *   +--Project(alias(v1 as v))
     *     +--Scan(v1)
     *
     * After rewriting, the plan should be:
     * Aggregate(bitmap_union_count(v) as cnt)
     *      +--Project(alias(mv_bitmap_union_v1 as v))
     *          +--Scan(mv_bitmap_union_v1)
     *
     * agg function in aggregate node: count(distinct v) as cnt
     * oldProject: v1 -> v
     * aggFuncMap: count(distinct v1) -> bitmap_union_count(mv_bitmap_union_v1)
     * </pre>
     */
    private List<NamedExpression> replaceAggOutput(
            LogicalAggregate<? extends Plan> agg,
            Optional<LogicalProject<? extends Plan>> oldProjectOpt,
            Map<AggregateFunction, AggregateFunction> aggFuncMap) {

        // oldProject: v1 -> v
        // replace agg function in aggregate node.
        // get the slot replace map from old project: v1 -> v
        Map<Expression, Slot> slotMap = oldProjectOpt
                .map(project -> project.getProjects()
                        .stream()
                        .filter(Alias.class::isInstance)
                        .collect(
                                Collectors.toMap(
                                        // Avoid cast to alias, retrieving the first child expression.
                                        alias -> alias.child(0),
                                        NamedExpression::toSlot
                                )
                        ))
                .orElse(ImmutableMap.of());

        // we have the project map: v1 -> v
        // aggFuncMap: count(distinct v1) -> bitmap_union_count(mv_bitmap_union_v1)
        // new agg replace map: count(distinct v) -> bitmap_union_count(mv_bitmap_union_v1)
        Map<AggregateFunction, AggregateFunction> newAggFuncMap = Maps.newHashMap();
        aggFuncMap.forEach((k, v) -> {
            newAggFuncMap.put((AggregateFunction) ExpressionUtils.replace(k, slotMap), v);
        });

        // replace agg output:  itcount(distinct v) as cnt -> bitmap_union_count(mv_bitmap_union_v1) as cnt.
        return agg.getOutputExpressions().stream()
                .map(expr -> (NamedExpression) ExpressionUtils.replace(expr, newAggFuncMap))
                .collect(ImmutableList.toImmutableList());
    }

    private List<NamedExpression> replaceProjectList(LogicalProject<? extends Plan> project, Map<Slot, Slot> slotMap) {
        return project.getProjects().stream()
                .map(expr -> (NamedExpression) ExpressionUtils.replace(expr, slotMap))
                .collect(ImmutableList.toImmutableList());
    }
}
