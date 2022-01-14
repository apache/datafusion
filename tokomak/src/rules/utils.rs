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

//! Contains utilities for writing rewrites that require more complex conditions

use crate::{expr::TokomakColumn, plan::TokomakLogicalPlan, TokomakAnalysis};

use datafusion::logical_plan::{DFField, DFSchema};

use egg::*;
///Logs the results of the condition
pub fn log_results<A: Analysis<TokomakLogicalPlan>>(
    name: String,
    c: impl Condition<TokomakLogicalPlan, A>,
) -> impl Condition<TokomakLogicalPlan, A> {
    move |egraph: &mut EGraph<TokomakLogicalPlan, A>, id: Id, subst: &Subst| {
        let res = c.check(egraph, id, subst);
        println!("[{}]={}", name, res);
        res
    }
}
/// l && r of Condition
pub fn and<A: Analysis<TokomakLogicalPlan>>(
    l: impl Condition<TokomakLogicalPlan, A>,
    r: impl Condition<TokomakLogicalPlan, A>,
) -> impl Fn(&mut EGraph<TokomakLogicalPlan, A>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakLogicalPlan, A>, id: Id, subst: &Subst| {
        l.check(egraph, id, subst) && r.check(egraph, id, subst)
    }
}
///l || r of Condition
pub fn or<A: Analysis<TokomakLogicalPlan>>(
    l: impl Condition<TokomakLogicalPlan, A>,
    r: impl Condition<TokomakLogicalPlan, A>,
) -> impl Fn(&mut EGraph<TokomakLogicalPlan, A>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakLogicalPlan, A>, id: Id, subst: &Subst| {
        l.check(egraph, id, subst) || r.check(egraph, id, subst)
    }
}

///Extracts the JoinKeys bound to keys or None if the eclass was not JoinKeys.
pub fn get_join_keys<A: Analysis<TokomakLogicalPlan>>(
    egraph: &EGraph<TokomakLogicalPlan, A>,
    keys: Id,
) -> Option<(&[Id], &[Id])> {
    if egraph[keys].nodes.len() != 1 {
        return None;
    }

    let [l, r] = match &egraph[keys].nodes[0] {
        TokomakLogicalPlan::JoinKeys(k) => Some([k[0], k[1]]),
        _ => None,
    }?;

    let left = get_expr_list(egraph, l)?;
    let right = get_expr_list(egraph, r)?;
    Some((left, right))
}

/// Returns the EList for the id if the EClass is a EList, otherwise returns none
pub fn get_expr_list<A: Analysis<TokomakLogicalPlan>>(
    egraph: &EGraph<TokomakLogicalPlan, A>,
    list_id: Id,
) -> Option<&[Id]> {
    match &egraph[list_id].nodes[0] {
        TokomakLogicalPlan::EList(l) => {
            assert!(egraph[list_id].nodes.len() == 1);
            Some(&l[..])
        }
        _ => None,
    }
}

///Converts TokomakLogicalPlan::JoinKeys to a filter predicate
pub fn keys_to_predicate<A: Analysis<TokomakLogicalPlan>>(
    join_keys: Var,
) -> impl Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst) -> Option<Id> {
    move |egraph: &mut EGraph<TokomakLogicalPlan, A>, subst: &Subst| -> Option<Id> {
        let (left, right) = get_join_keys(egraph, subst[join_keys])?;
        assert!(left.len() == right.len());

        let (left, right) = (left.to_vec(), right.to_vec());
        let mut predicate = egraph.add(TokomakLogicalPlan::Eq([left[0], right[0]]));
        for (l, r) in left.iter().zip(right.iter()).skip(1) {
            let other = egraph.add(TokomakLogicalPlan::Eq([*l, *r]));
            predicate = egraph.add(TokomakLogicalPlan::And([predicate, other]));
        }
        Some(predicate)
    }
}

///Returns true if the var bound is a TokomakScalar
pub fn is_scalar(
    var: Var,
) -> impl Fn(&mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>,
          _id: Id,
          subst: &Subst| {
        egraph[subst[var]]
            .nodes
            .iter()
            .flat_map(|f| match f {
                TokomakLogicalPlan::Scalar(_) => Some(()),
                _ => None,
            })
            .next()
            .is_some()
    }
}
///Gets the schema of the eclass bound to plan or returns None
pub fn get_plan_schema<'a>(
    egraph: &'a EGraph<TokomakLogicalPlan, TokomakAnalysis>,
    subst: &Subst,
    plan: Var,
) -> Option<&'a DFSchema> {
    match &egraph[subst[plan]].data {
        crate::TData::Schema(s) => Some(s),
        _ => None,
    }
}
///Gets the column expression bound to col or returns None if it was of a different type.
pub fn get_column(
    egraph: &EGraph<TokomakLogicalPlan, TokomakAnalysis>,
    subst: &Subst,
    col: Var,
) -> Option<TokomakColumn> {
    let eclass = &egraph[subst[col]];
    if eclass.nodes.len() != 1 {
        return None;
    }
    let column = match &eclass.nodes[0] {
        TokomakLogicalPlan::Column(c) => Some(c),
        _ => None,
    };
    assert!(eclass.nodes.len() == 1);

    Some(column?.clone())
}
///Gets the field from the schema that corresponds to the column or returns None if it could not be found.
pub fn get_field<'a>(schema: &'a DFSchema, col: &TokomakColumn) -> Option<&'a DFField> {
    match col.relation {
        Some(q) => schema
            .field_with_qualified_name(q.as_str(), col.name.as_str())
            .ok(),
        None => schema.field_with_unqualified_name(col.name.as_str()).ok(),
    }
}
///Checks if the TokomakColumn is from the schema
pub fn col_from_plan(schema: &DFSchema, col: &TokomakColumn) -> bool {
    get_field(schema, col).is_some()
}
///Checks if the col var is from the plan bound to the plan var
pub fn is_column_from_plan(
    col: Var,
    plan: Var,
) -> impl Fn(&mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>,
          _id: Id,
          subst: &Subst| {
        let plan_schema = get_plan_schema(egraph, subst, plan);
        let plan_schema = match plan_schema {
            Some(s) => s,
            None => return false,
        };

        assert!(egraph[subst[col]].nodes.len() == 1);
        let column = match &egraph[subst[col]].nodes[0] {
            TokomakLogicalPlan::Column(c) => Some(c),
            _ => None,
        };

        let column = match column {
            Some(c) => c,
            None => return false,
        };

        let field = match column.relation {
            Some(q) => {
                plan_schema.field_with_qualified_name(q.as_str(), column.name.as_str())
            }
            None => plan_schema.field_with_unqualified_name(column.name.as_str()),
        };

        field.is_ok()
    }
}

///prints the result of the wrapped Condition to stdout
pub fn log(
    name: String,
    c: impl Condition<TokomakLogicalPlan, TokomakAnalysis>,
) -> impl Condition<TokomakLogicalPlan, TokomakAnalysis> {
    move |egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>,
          id: Id,
          subst: &Subst|
          -> bool {
        let res = c.check(egraph, id, subst);
        println!("Executed condition {} with {}", name, res);
        res
    }
}
///Prints the result of the wrapped Condition to stdout if it was successful
pub fn log_success(
    name: String,
    c: impl Condition<TokomakLogicalPlan, TokomakAnalysis>,
) -> impl Condition<TokomakLogicalPlan, TokomakAnalysis> {
    move |egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>,
          id: Id,
          subst: &Subst|
          -> bool {
        let res = c.check(egraph, id, subst);
        if res {
            println!("Executed condition {} with {}", name, res);
        }
        res
    }
}
///Adds left_col and right_col to join_keys if they are from different plan schemas. Adds the created JoinKeys to Subst as output.
pub fn add_to_keys(
    left_table: Var,
    right_table: Var,
    left_col: Var,
    right_col: Var,
    join_keys: Var,
    output: Var,
) -> impl Fn(&mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, &mut Subst) -> Option<()> {
    move |eg: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>,
          substs: &mut Subst|
          -> Option<()> {
        let lschema = get_plan_schema(eg, substs, left_table)?;
        let rschema = get_plan_schema(eg, substs, right_table)?;
        let mut lcol = get_column(eg, substs, left_col)?;
        let mut rcol = get_column(eg, substs, right_col)?;

        let (l, r) = if let (Some(_), Some(_)) =
            (get_field(lschema, &lcol), get_field(rschema, &rcol))
        {
            (left_col, right_col)
        } else if let (Some(_), Some(_)) =
            (get_field(lschema, &rcol), get_field(rschema, &lcol))
        {
            std::mem::swap(&mut lcol, &mut rcol);
            (right_col, left_col)
        } else {
            return None;
        };
        let (left_keys, right_keys) = get_join_keys(eg, substs[join_keys])?;
        let (mut left_keys, mut right_keys) = (left_keys.to_vec(), right_keys.to_vec());
        left_keys.push(substs[l]);
        right_keys.push(substs[r]);
        let lkeys_id = eg.add(TokomakLogicalPlan::EList(left_keys.into_boxed_slice()));
        let rkeys_id = eg.add(TokomakLogicalPlan::EList(right_keys.into_boxed_slice()));
        let id = eg.add(TokomakLogicalPlan::JoinKeys([lkeys_id, rkeys_id]));
        substs.insert(output, id);
        Some(())
    }
}

///Appends a scalar value to EList
pub fn append_in_list<A: Analysis<TokomakLogicalPlan> + 'static>(
    list_var: Var,
    item_var: Var,
    output_var: Var,
) -> impl Fn(&mut EGraph<TokomakLogicalPlan, A>, &mut Subst) -> Option<()> {
    move |eg: &mut EGraph<TokomakLogicalPlan, A>, substs: &mut Subst| -> Option<()> {
        let mut lists = eg[substs[list_var]]
            .nodes
            .iter()
            .flat_map(|node| match node {
                TokomakLogicalPlan::EList(list) => Some(list),
                _ => None,
            })
            .collect::<Vec<_>>();
        if lists.is_empty() {
            return None;
        }
        assert!(eg[substs[item_var]].nodes.len() == 1);
        //Check if it has a scalar representation
        match &eg[substs[item_var]].nodes[0] {
            TokomakLogicalPlan::Scalar(_) => (),
            _ => return None,
        }

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
///Appends the id bound to item_var to list_var. item_var must be an expression.
pub fn append_to_elist<A: Analysis<TokomakLogicalPlan> + 'static>(
    list_var: Var,
    item_var: Var,
) -> impl Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst) -> Option<Id> {
    move |eg: &mut EGraph<TokomakLogicalPlan, A>, substs: &Subst| -> Option<Id> {
        let mut lists = eg[substs[list_var]]
            .nodes
            .iter()
            .flat_map(|node| match node {
                TokomakLogicalPlan::EList(list) => Some(list),
                _ => None,
            })
            .collect::<Vec<_>>();
        if lists.is_empty() {
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

///Appends the id bound to item_var to list_var. item_var must be a plan
pub fn append_to_plist<A: Analysis<TokomakLogicalPlan> + 'static>(
    list_var: Var,
    item_var: Var,
) -> impl Fn(&mut EGraph<TokomakLogicalPlan, A>, &Subst) -> Option<Id> {
    move |eg: &mut EGraph<TokomakLogicalPlan, A>, substs: &Subst| -> Option<Id> {
        let mut lists = eg[substs[list_var]]
            .nodes
            .iter()
            .flat_map(|node| match node {
                TokomakLogicalPlan::PList(list) => Some(list),
                _ => None,
            })
            .collect::<Vec<_>>();
        if lists.is_empty() {
            return None;
        }
        lists.sort_by_key(|l| l.len());
        let longest = lists.last().unwrap();
        let mut new_list = Vec::with_capacity(longest.len() + 1);
        new_list.extend(longest.iter().cloned());
        new_list.push(eg[substs[item_var]].id);
        Some(eg.add(TokomakLogicalPlan::PList(new_list.into_boxed_slice())))
    }
}
