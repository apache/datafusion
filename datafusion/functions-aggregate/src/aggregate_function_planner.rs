use datafusion_expr::{
    expr, lit,
    planner::{PlannerResult, RawAggregateFunction, UserDefinedSQLPlanner},
    Expr,
};

pub struct AggregateFunctionPlanner;

impl UserDefinedSQLPlanner for AggregateFunctionPlanner {
    fn plan_aggregate_function(
        &self,
        aggregate_function: RawAggregateFunction,
    ) -> datafusion_common::Result<PlannerResult<RawAggregateFunction>> {
        let RawAggregateFunction {
            udf,
            args,
            distinct,
            filter,
            order_by,
            null_treatment,
        } = aggregate_function.clone();

        if udf.name() == "count" && args.is_empty() {
            return Ok(PlannerResult::Planned(Expr::AggregateFunction(
                expr::AggregateFunction::new_udf(
                    udf.clone(),
                    vec![lit(1).alias("")],
                    distinct,
                    filter.clone(),
                    order_by.clone(),
                    null_treatment.clone(),
                ),
            )));
        }

        Ok(PlannerResult::Original(aggregate_function.clone()))
    }
}
