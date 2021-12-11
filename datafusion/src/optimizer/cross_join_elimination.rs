//! This module contains an optimizer that transforms Filter<-CrossJoin into Filter<-InnerJoin

use std::collections::HashSet;
use std::sync::Arc;


use crate::logical_plan::{Column, DFSchema, Expr, JoinConstraint, JoinType, LogicalPlan};
use crate::optimizer::constant_folding::Simplifier;
use crate::scalar::ScalarValue;

use super::optimizer::OptimizerRule;
use super::utils;

/// This transforms Cross joins followed by filters into Inner joins followed by filters.
/// Filter predicate: #table1.key = #table2.key ... other predicates
///     CrossJoin left: #table1  right: #table2
/// Into
/// Filter predicate: other predicates
/// InnerJoin left: #table1 right:#table2 on: [(#table1.key, #table2.key)]
pub struct CrossJoinElminator;
impl CrossJoinElminator{
    #[allow(missing_docs)]
    pub fn new() -> CrossJoinElminator{
        Self{}
    }
}

impl OptimizerRule for CrossJoinElminator{
    fn optimize(
        &self,
        plan: &crate::logical_plan::LogicalPlan,
        execution_props: &crate::execution::context::ExecutionProps,
    ) -> crate::error::Result<crate::logical_plan::LogicalPlan> {
        let p = match plan{
            crate::logical_plan::LogicalPlan::Filter { predicate, input } =>{
                let new_plan = match input.as_ref(){
                    crate::logical_plan::LogicalPlan::CrossJoin { left, right, schema } => {
                        let all_schemas = input.all_schemas();
                        find_join_keys(left, right,predicate, all_schemas).map(|(new_filter_predicate, join_keys)|{    
                            
                            let new_input = LogicalPlan::Join{left:left.clone(), right:right.clone(), join_type: JoinType::Inner, join_constraint: JoinConstraint::On, on:join_keys,schema:schema.clone()};
                            LogicalPlan::Filter{predicate: new_filter_predicate, input: Arc::new(new_input)}
                        })
                    },
                    _ => None
                };
                
                match new_plan{
                    Some(p)=>{
                        p
                    }   
                    None => utils::from_plan(plan,&plan.expressions(), &[self.optimize(input, execution_props)?])?
                }
            },
            LogicalPlan::Analyze { input,.. } |
            LogicalPlan::Window { input,.. } |
            LogicalPlan::Aggregate { input,.. }|
            LogicalPlan::Sort { input,.. }  |
            LogicalPlan::Limit {  input,.. } |
            LogicalPlan::Repartition { input, .. } |
            LogicalPlan::Projection {  input, .. } => utils::from_plan(plan,&plan.expressions(), &[self.optimize(input, execution_props)?])?,
            LogicalPlan::Join { left, right, on, join_type, join_constraint, schema } => {
                
                let left = Arc::new(self.optimize(left, execution_props)?);
                let right = Arc::new(self.optimize(right, execution_props)?);
                LogicalPlan::Join{left, right, on: on.clone(), join_type: *join_type, join_constraint: *join_constraint, schema: schema.clone()}
            },
            LogicalPlan::CrossJoin { left, right, schema } => {
                let left = Arc::new(self.optimize(left, execution_props)?);
                let right = Arc::new(self.optimize(right, execution_props)?);
                LogicalPlan::CrossJoin{left, right,schema: schema.clone()}

            },
            LogicalPlan::Union { inputs, schema, alias } => {
                let inputs = inputs.iter().map(|p| self.optimize(p, execution_props)).collect::<Result<Vec<_>,_>>()?;
                LogicalPlan::Union{inputs, schema: schema.clone(), alias: alias.clone()}
            },
            LogicalPlan::TableScan { ..} |
            LogicalPlan::EmptyRelation { .. } |
            LogicalPlan::CreateExternalTable {..}|
            LogicalPlan::Values { ..} |
            LogicalPlan::Explain { .. } => plan.clone(),
            
            LogicalPlan::Extension { .. } => plan.clone()
            
        };
        Ok(p)
    }

    fn name(&self) -> &str {
        "CrossJoinEliminator" 
    }
}

fn find_join_keys(left: &Arc<LogicalPlan>, right: &Arc<LogicalPlan>, predicate: &Expr, all_schemas: Vec<&Arc<DFSchema>>)->Option<(Expr,Vec<(Column, Column)>)>{
    let mut output = HashSet::new();
    let mut new_predicate = predicate.clone();
    find_join_and_replace_join_keys_rec(&mut new_predicate, &mut output, left.schema().as_ref(), right.schema().as_ref());
    let mut rewriter = Simplifier::new(all_schemas);
    println!("THe joine keys are {:?}", output);
    let new_predicate = new_predicate.rewrite(&mut rewriter).ok()?;
    if output.is_empty(){
        None
    }else{
        Some((new_predicate,output.into_iter().collect()))
    }
}

fn find_join_and_replace_join_keys_rec(predicate: &mut Expr,  output:&mut HashSet<(Column, Column)>, left_schema: &DFSchema, right_schema:&DFSchema){
    match predicate{
        Expr::Alias(c, _) => find_join_and_replace_join_keys_rec(c, output, left_schema, right_schema),
        Expr::BinaryExpr { left, op, right } => match op{
            crate::logical_plan::Operator::Eq => {
                match (left.as_ref(), right.as_ref()){
                    (Expr::Column(l), Expr::Column(r)) =>{
                        let mut remove_eq = false;
                        if let (Ok(_), Ok(_)) = (left_schema.index_of_column(l), right_schema.index_of_column(r)){
                            output.insert((l.clone(),r.clone()));
                            remove_eq = true;
                        }else if let (Ok(_), Ok(_)) = ( left_schema.index_of_column(r),right_schema.index_of_column(l)){
                            output.insert((r.clone(), l.clone()));
                            remove_eq = true;
                        }
                        if remove_eq{
                            *predicate = Expr::Literal(ScalarValue::Boolean(Some(true)));
                        }
                    }
                    _ =>{
                        find_join_and_replace_join_keys_rec(left, output, left_schema, right_schema);
                        find_join_and_replace_join_keys_rec(right, output, left_schema, right_schema);
                    }
                }
            },
            crate::logical_plan::Operator::And => {
                find_join_and_replace_join_keys_rec(left, output, left_schema, right_schema);
                find_join_and_replace_join_keys_rec(right, output, left_schema, right_schema);
            },
            _ => (),
        },
        Expr::Column(_)|
        Expr::ScalarVariable(_)|
        Expr::Literal(_) |
        Expr::Not(_) |
        Expr::IsNotNull(_)|
        Expr::IsNull(_) |
        Expr::Negative(_)|
        Expr::GetIndexedField {..} |
        Expr::Between { .. }|
        Expr::Case { .. }|
        Expr::Cast { .. } |
        Expr::TryCast { .. } |
        Expr::Sort { .. } |
        Expr::ScalarFunction {.. } |
        Expr::ScalarUDF { .. }|
        Expr::AggregateFunction { .. } |
        Expr::WindowFunction { .. } |
        Expr::AggregateUDF { ..}|
        Expr::InList { ..} |
        Expr::Wildcard => (),
    }
}

