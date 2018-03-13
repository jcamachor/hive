package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

/**
 * This rule will perform a rewriting to prepare the plan for incremental
 * view maintenance in case there is no aggregation operator, so we can
 * avoid the INSERT OVERWRITE and use a INSERT statement instead.
 * In particular, it removes the union branch that reads the old data from
 * the materialization, and keeps the branch that will read the new data.
 */
public class HiveNoAggregateIncrementalRewritingRule extends RelOptRule {

  public static final HiveNoAggregateIncrementalRewritingRule INSTANCE =
      new HiveNoAggregateIncrementalRewritingRule();

  private HiveNoAggregateIncrementalRewritingRule() {
    super(operand(Union.class, any()),
        HiveRelFactories.HIVE_BUILDER, "HiveNoAggregateIncrementalRewritingRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Union union = call.rel(0);
    // First branch is query, second branch is MV
    RelNode newNode = call.builder()
        .push(union.getInput(0))
        .convert(union.getRowType(), false)
        .build();
    call.transformTo(newNode);
  }

}
