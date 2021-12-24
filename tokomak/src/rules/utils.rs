use datafusion::{error::DataFusionError, execution::context::ExecutionProps, logical_plan::{Expr, Operator, DFSchema, DFField}, physical_plan::expressions::{ BinaryExpr, CastExpr}, scalar::ScalarValue, arrow::ffi::ArrowArrayRef, datasource::TableProvider};
use datafusion::arrow::datatypes::DataType;
use egg::*;
use crate::{  scalar::TokomakScalar, plan::{TokomakLogicalPlan, Table, TableScan, JoinKeys}, TokomakAnalysis, expr::TokomakColumn};


use std::{convert::TryInto, sync::Arc};

type DFResult<T>=Result<T, DataFusionError>;






pub fn log_results<A: Analysis<TokomakLogicalPlan>>(name: String, c: impl Condition<TokomakLogicalPlan, A>)->impl Condition<TokomakLogicalPlan, A>{
    move |egraph: &mut EGraph<TokomakLogicalPlan, A>, id:Id,subst: &Subst|{
        let res = c.check(egraph, id, subst);
        println!("[{}]={}", name, res);
        res       
     }
}

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

pub fn get_join_keys<A: Analysis<TokomakLogicalPlan>>(egraph: &EGraph<TokomakLogicalPlan,A>, keys: Id)->Option<&JoinKeys>{
    match &egraph[keys].nodes[0]{
        TokomakLogicalPlan::JoinKeys(k)=>Some(k),
        _=> None,
    }
}

pub fn revers_keys<A: Analysis<TokomakLogicalPlan>>(join_keys: Var, output: Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, A>, &mut Subst) -> Option<()>{
    move |egraph: &mut EGraph<TokomakLogicalPlan, A>, subst: &mut Subst|->Option<()>{
        let keys = egraph[subst[join_keys]].nodes.iter().flat_map(|f|match f{
            TokomakLogicalPlan::JoinKeys(keys)=>Some(keys),
            _=>None,
        }).next()?.as_slice();
        if keys.is_empty(){
            return None;
        }
        let mut new_keys = Vec::with_capacity(keys.len());
        for chunk in keys.chunks_exact(2){
            let l = chunk[0];
            let r = chunk[1];
            new_keys.push(r);
            new_keys.push(l);
        }
       let id = egraph.add(TokomakLogicalPlan::JoinKeys(JoinKeys(new_keys.into_boxed_slice())));
       subst.insert(output, id);
       Some(())
    }
}

pub fn keys_to_predicate<A: Analysis<TokomakLogicalPlan>>(join_keys: Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst) -> Option<Id>{
    move |egraph: &mut EGraph<TokomakLogicalPlan, A>, subst: &Subst|->Option<Id>{
        let keys = egraph[subst[join_keys]].nodes.iter().flat_map(|f|match f{
            TokomakLogicalPlan::JoinKeys(keys)=>Some(keys),
            _=>None,
        }).next()?;
        if keys.is_empty(){
            return None;
        }
        let keys = keys.0.clone();
        let mut predicate = egraph.add(TokomakLogicalPlan::Eq([keys[0], keys[1]]));
        for chunk in keys.chunks_exact(2).skip(1){
            let l = chunk[0];
            let r = chunk[1];
            let other = egraph.add(TokomakLogicalPlan::Eq([l, r]));
            predicate = egraph.add(TokomakLogicalPlan::And([predicate, other]));
        }
        Some(predicate)
    }
}
pub fn is_scalar(var: Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, Id, &Subst) -> bool{
    move |egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, _id:Id, subst: &Subst|{
        egraph[subst[var]].nodes.iter().flat_map(|f| match f{
            TokomakLogicalPlan::Scalar(_)=>Some(()),
            _ => None
        }).next().is_some()
    }
}

pub fn get_plan_schema<'a>(egraph: &'a EGraph<TokomakLogicalPlan, TokomakAnalysis>,subst: &Subst, plan: Var)->Option<&'a DFSchema>{
    match &egraph[subst[plan]].data{
        crate::TData::Schema(s) => Some(s),
        _=>{
            None
        },
    }
}

pub fn get_column(egraph: &EGraph<TokomakLogicalPlan, TokomakAnalysis>,subst: &Subst, col: Var)->Option<TokomakColumn>{
    let eclass = &egraph[subst[col]];
    if eclass.nodes.len() != 1{
        return None;
    }
    let column = match &eclass.nodes[0]{
        TokomakLogicalPlan::Column(c)=> Some(c),
        _ => None
    };
    assert!(eclass.nodes.len()==1);


    Some(column?.clone())
}

pub fn get_field<'a>(schema: &'a DFSchema, col: &TokomakColumn)->Option<&'a DFField>{
    match col.relation{
        Some(q) => schema.field_with_qualified_name(q.as_str(),col.name.as_str()).ok(),
        None => schema.field_with_unqualified_name(col.name.as_str()).ok(),
    }
}

pub fn col_from_plan(schema: &DFSchema, col: &TokomakColumn)->bool{
    get_field(schema, col).is_some()
}



pub fn is_column_from_plan(col: Var, plan: Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, Id, &Subst) -> bool{
    move |egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, _id:Id,subst: &Subst|{
        let plan_schema = get_plan_schema(egraph, subst, plan);
        let plan_schema= match plan_schema{
            Some(s)=>s,
            None => return false,
        };
        
        assert!(egraph[subst[col]].nodes.len() == 1);
        let column =  match & egraph[subst[col]].nodes[0]{
            TokomakLogicalPlan::Column(c)=>Some(c),
            _=>None,
        };

        let column = match column{
            Some(c) =>c,
            None => {
                return false
            },
        };

        let field = match column.relation{
            Some(q) => plan_schema.field_with_qualified_name(q.as_str(),column.name.as_str()),
            None => plan_schema.field_with_unqualified_name(column.name.as_str()),
        };
        
        field.is_ok()
    }
}

pub fn log(name: String, c: impl Condition<TokomakLogicalPlan, TokomakAnalysis>)->impl Condition<TokomakLogicalPlan, TokomakAnalysis>{
    move |egraph: &mut EGraph<TokomakLogicalPlan,TokomakAnalysis>, id: Id, subst: &Subst|->bool{
        let res = c.check(egraph, id, subst);
        println!("Executed condition {} with {}", name, res);
        res
    }
}

pub fn log_success(name: String, c: impl Condition<TokomakLogicalPlan, TokomakAnalysis>)->impl Condition<TokomakLogicalPlan, TokomakAnalysis>{
    move |egraph: &mut EGraph<TokomakLogicalPlan,TokomakAnalysis>, id: Id, subst: &Subst|->bool{
        let res = c.check(egraph, id, subst);
        if res{
            println!("Executed condition {} with {}", name, res);
        }
        res
    }
}


pub fn add_to_keys(left_table: Var, right_table: Var, left_col: Var,right_col:Var, join_keys: Var, output: Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, &mut Subst)->Option<()>{
    move |eg: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, substs: &mut Subst|->Option<()>{
        let lschema = get_plan_schema(eg, substs, left_table)?;
        let rschema = get_plan_schema(eg, substs, right_table)?;
        let mut lcol = get_column(eg, substs, left_col)?;
        let mut rcol = get_column(eg, substs, right_col)?;

        let (l,r) = if let (Some(_), Some(_)) = (get_field(&lschema, &lcol), get_field(&rschema, &rcol)){
            (left_col, right_col)
        }else if let (Some(_), Some(_)) = (get_field(&lschema, &rcol), get_field(&rschema, &lcol)){
            std::mem::swap(&mut lcol, &mut rcol);
            (right_col, left_col)
        }else{
            return None;
        };
        let keys =match eg[substs[join_keys]].nodes.iter().flat_map(|p| match p{
            TokomakLogicalPlan::JoinKeys(keys)=> Some(keys),
            _ => None,
        }).next(){
            Some(k)=>k,
            None => {
                return None;
            }
        };
        let mut new_keys = Vec::with_capacity(keys.0.len());
        for id in keys.0.iter(){
            new_keys.push(*id);
        }
        
        new_keys.push(substs[l]);
        new_keys.push(substs[r]);
        let keys = if JoinKeys::can_be_length(new_keys.len()){
            JoinKeys::from_vec(new_keys)
        }else{
            return None;
        };
        let id = eg.add(TokomakLogicalPlan::JoinKeys(keys));
        substs.insert(output, id);
        Some(())
    }

}

pub fn append_in_list<A: Analysis<TokomakLogicalPlan> + 'static>(list_var: Var, item_var: Var, output_var: Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, A>, & mut Subst)->Option<()>{
    move |eg: &mut EGraph<TokomakLogicalPlan, A>, substs: &mut Subst|->Option<()>{
        let mut lists = eg[substs[list_var]].nodes.iter().flat_map(|node| match node{
            TokomakLogicalPlan::EList(list)=>Some(list),
            _ => {
                None
            },
        }).collect::<Vec<_>>();
        if lists.is_empty(){
            return None;
        }
        //Check if it has a scalar representation
        eg[substs[item_var]].nodes.iter().flat_map(|f|match f {
            TokomakLogicalPlan::Scalar(s)=> Some(()),
            _ => None
        }).next()?;

        lists.sort_by_key(|l| l.len());
        let longest = lists.last().unwrap();
        let mut new_list = Vec::with_capacity(longest.len() + 1);
        new_list.extend(longest.iter().cloned());
        new_list.push(eg[substs[item_var]].id);
        let transformed = eg.add(TokomakLogicalPlan::EList(new_list.into_boxed_slice()));
        substs.insert(output_var, transformed);
        Some(())
    }
}



pub fn append<A: Analysis<TokomakLogicalPlan> + 'static>(list_var: Var, item_var: Var)->impl Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst)->Option<Id>{
    move |eg: &mut EGraph<TokomakLogicalPlan, A>, substs: &Subst|->Option<Id>{
        let mut lists = eg[substs[list_var]].nodes.iter().flat_map(|node| match node{
            TokomakLogicalPlan::EList(list)=>Some(list),
            _ => {
                None
            },
        }).collect::<Vec<_>>();
        if lists.is_empty(){
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
