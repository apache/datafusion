use crate::planner::{
    object_name_to_table_reference, ContextProvider, PlannerContext, SqlToRel,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::TableFactor;

mod join;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    fn create_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let (plan, alias) = match relation {
            TableFactor::Table { name, alias, .. } => {
                // normalize name and alias
                let table_ref = object_name_to_table_reference(name)?;
                let table_name = table_ref.to_string();
                let cte = planner_context.ctes.get(&table_name);
                (
                    match (
                        cte,
                        self.schema_provider.get_table_provider((&table_ref).into()),
                    ) {
                        (Some(cte_plan), _) => Ok(cte_plan.clone()),
                        (_, Ok(provider)) => {
                            LogicalPlanBuilder::scan(&table_name, provider, None)?.build()
                        }
                        (None, Err(e)) => Err(e),
                    }?,
                    alias,
                )
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let logical_plan = self.query_to_plan(*subquery, planner_context)?;
                (logical_plan, alias)
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => (
                self.plan_table_with_joins(*table_with_joins, planner_context)?,
                alias,
            ),
            // @todo Support TableFactory::TableFunction?
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported ast node {relation:?} in create_relation"
                )));
            }
        };
        if let Some(alias) = alias {
            self.apply_table_alias(plan, alias)
        } else {
            Ok(plan)
        }
    }
}
