use datafusion::{error::DataFusionError, execution::context::ExecutionProps, logical_plan::{Expr, Operator}, physical_plan::expressions::{ BinaryExpr, CastExpr}, scalar::ScalarValue, arrow::ffi::ArrowArrayRef, datasource::TableProvider};
use datafusion::arrow::datatypes::DataType;
use egg::*;
use datafusion::optimizer::utils::ConstEvaluator;
use crate::{  scalar::TokomakScalar, plan::{TokomakLogicalPlan, Table, TableScan}, TokomakAnalysis};

use super::super::TokomakExpr;
use std::convert::TryInto;



pub fn and<A: Analysis<TokomakLogicalPlan>>(l: impl Condition<TokomakLogicalPlan, A>, r: impl  Condition<TokomakLogicalPlan, A>)->impl Fn(&mut EGraph<TokomakLogicalPlan, A>, Id, &Subst) -> bool{
    move |egraph: &mut EGraph<TokomakLogicalPlan, A>, id:Id,subst: &Subst|{
       l.check(egraph, id, subst) && r.check(egraph, id, subst)        
    }
}

pub fn or<A: Analysis<TokomakLogicalPlan>>(l: impl Condition<TokomakLogicalPlan, A>, r: impl  Condition<TokomakLogicalPlan, A>)->impl Fn(&mut EGraph<TokomakLogicalPlan, A>, Id, &Subst) -> bool{
    move |egraph: &mut EGraph<TokomakLogicalPlan, A>, id:Id,subst: &Subst|{
        l.check(egraph, id, subst) || r.check(egraph, id, subst)
    }
}


pub fn not<A: Analysis<TokomakLogicalPlan>>(inner: impl Condition<TokomakLogicalPlan, A>)->impl Fn(&mut EGraph<TokomakLogicalPlan, A>, Id, &Subst) -> bool{
    move |egraph: &mut EGraph<TokomakLogicalPlan, A>, id:Id,subst: &Subst|{
        !inner.check(egraph, id, subst)
    }
}


pub fn column_from_plan(col: Var, plan: Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, Id, &Subst) -> bool{
    move |egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, id:Id,subst: &Subst|{
        //println!("Evaluated column_from_plan");
        let plan_schema =match egraph.analysis.plan_schema_map.get(&egraph[subst[plan]].id){
            Some(schema)=>schema,
            None => {
                return false
            },
        };
        
        let columns =  egraph[subst[col]].nodes.iter().flat_map(|p| match p{
            TokomakLogicalPlan::Column(c)=> Some(c),
            _ => None
        }).collect::<Vec<_>>();
        if columns.is_empty(){
            return false
        }

        let col = columns[0].into();
        let field = plan_schema.field_from_column(&col);
        field.is_ok()
    }
}




pub fn append<A: Analysis<TokomakLogicalPlan> + 'static>(list_var: Var, item_var: Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst)->Option<Id>{
    move |eg: &mut EGraph<TokomakLogicalPlan, A>, substs: &Subst|->Option<Id>{
        println!("Appending {} to {}", item_var, list_var);
        let mut lists = eg[substs[list_var]].nodes.iter().flat_map(|node| match node{
            TokomakLogicalPlan::EList(list)=>Some(list),
            _ => {
                None
            },
        }).collect::<Vec<_>>();
        if lists.is_empty(){
            println!("Enode had not list representations");
            return None;
        }
        lists.sort_by_key(|l| l.len());
        let longest = lists.last().unwrap();
        let mut new_list = Vec::with_capacity(longest.len() + 1);
        new_list.extend(longest.iter().cloned());
        new_list.push(eg[substs[item_var]].id);
        Some(eg.add(TokomakLogicalPlan::EList(new_list.into_boxed_slice())))
    }
}
