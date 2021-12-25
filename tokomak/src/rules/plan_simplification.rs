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
use crate::{
    pattern::{conditional_rule, pattern, transforming_pattern, twoway_pattern},
    plan::TokomakLogicalPlan,
    Tokomak, TokomakAnalysis, PLAN_SIMPLIFICATION_RULES,
};
use datafusion::error::DataFusionError;
use egg::*;
use log::info;

use super::utils::*;

pub(crate) fn generate_plan_simplification_rules(
) -> Result<Vec<Rewrite<TokomakLogicalPlan, TokomakAnalysis>>, DataFusionError> {
    let l: Var = "?l".parse().unwrap();
    let r: Var = "?r".parse().unwrap();
    let lcol: Var = "?lcol".parse().unwrap();
    let rcol: Var = "?rcol".parse().unwrap();
    let c1: Var = "?c1".parse().unwrap();
    let c2: Var = "?c2".parse().unwrap();

    let newkeys = "?newkeys".parse().unwrap();

    let keys: Var = "?keys".parse().unwrap();
    let inner_keys = "?inner_keys".parse().unwrap();
    let outer_keys = "?outer_keys".parse().unwrap();
    let mut rules = vec![
        conditional_rule(
            "plan-filter-crossjoin->innerjoin",
            "(filter  \
                (cross_join ?l ?r ) \
                (and \
                    (= ?lcol ?rcol) \
                    ?pred \
                ) \
            )",
            "(filter (inner_join ?l ?r (keys (elist ?lcol) (elist ?rcol)) false) ?pred)",
            and(is_column_from_plan(lcol, l), is_column_from_plan(rcol, r)),
        )?,
        transforming_pattern(
            "plan-inner-join-key-add",
            "(filter (inner_join ?l ?r ?keys ?n_eq_n)  (and (= ?lcol ?rcol) ?other))",
            "(filter (inner_join ?l ?r ?newkeys ?n_eq_n) ?other)",
            add_to_keys(l, r, lcol, rcol, keys, newkeys),
            &[newkeys],
        )?,
        pattern("plan-remove-filter-true", "(filter ?input true)", "?input")?,
        pattern(
            "plan-replace-filter-false",
            "(filter ?input false)",
            "(empty_relation false)",
        )?,
        pattern(
            "plan-merge-filters",
            "(filter (filter ?input ?inner_pred) ?outer_pred)",
            "(filter ?input (and ?inner_pred ?outer_pred))",
        )?,
        transforming_pattern(
            "inner-cross-inner-2",
            "(inner_join (cross_join ?c1 ?c2) ?r ?keys ?n_eq_n)",
            "(inner_join (inner_join ?r ?c1 ?inner_keys false) ?c2 ?outer_keys ?n_eq_n)",
            inner_join_cross_join_through(c1, c2, r, keys, inner_keys, outer_keys),
            &[inner_keys, outer_keys],
        )?,
        pattern(
            "plan-crossjoin-commutative",
            "(cross_join ?l ?r)",
            "(cross_join ?r ?l)",
        )?,
        pattern(
            "plan-innerjoin-commutative",
            "(inner_join ?l ?r (keys ?lkeys ?rkeys) ?n_eq_n)",
            "(inner_join ?r ?l (keys ?rkeys ?lkeys) ?n_eq_n)",
        )?,
        pattern(
            "plan-raise-filter",
            "(cross_join ?t1 (filter (cross_join ?t2 ?t3) ?pred))",
            "(filter (cross_join ?t1 (cross_join ?t2 ?t3)) ?pred)",
        )?,
        pattern(
            "plan-filter-raising-left",
            "(cross_join (filter ?l ?pred) ?r)",
            "(filter (cross_join ?l ?r) ?pred)",
        )?,
        pattern(
            "plan-filter-raising-right",
            "(cross_join  ?l (filter ?r ?pred))",
            "(filter (cross_join ?l ?r) ?pred)",
        )?,
        conditional_rule(
            "plan-filter-lower-new-filter",
            "(filter (cross_join ?l ?r) (and (= ?c1 ?c2) ?other))",
            "(filter (cross_join (filter ?l (and (= ?c1 ?c2) true)) ?r) ?other)",
            and(is_column_from_plan(c1, l), is_column_from_plan(c2, l)),
        )?,
        conditional_rule(
            "plan-filter-lower-existing-filter",
            "(filter \
            (cross_join \
                (filter ?l ?existing ) \
                ?r\
            )\
            (and \
                (= ?c1 ?c2) \
                ?other\
            )\
        )",
            "(filter (cross_join (filter ?l (and (= ?c1 ?c2) ?existing)) ?r) ?other)",
            and(is_column_from_plan(c1, l), is_column_from_plan(c2, l)),
        )?,
    ];
    rules.extend(twoway_pattern(
        "plan-crossjoin-rotate",
        "(cross_join ?t1 (cross_join ?t2 ?t3))",
        "(cross_join (cross_join ?t1 ?t2) ?t3)",
    )?);

    Ok(rules)
}
//"(inner_join (cross_join ?c1 ?c2) ?r ?keys)" => "(inner_join (inner_join ?c1 ?r ?inner_keys) ?c2 ?outer_keys)",
fn inner_join_cross_join_through(
    c1: Var,
    c2: Var,
    r: Var,
    keys: Var,
    inner_keys: Var,
    outer_keys: Var,
) -> impl Fn(&mut EGraph<TokomakLogicalPlan, TokomakAnalysis>, &mut Subst) -> Option<()> {
    move |egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>,
          subst: &mut Subst|
          -> Option<()> {
        assert!(egraph[subst[keys]].nodes.len() == 1);
        let (left_keys, right_keys) = get_join_keys(egraph, subst[keys])?;
        let cross1_schema = get_plan_schema(egraph, subst, c1)?;
        let cross2_schema = get_plan_schema(egraph, subst, c2)?;
        let rschema = get_plan_schema(egraph, subst, r)?;
        let mut c1_r_right_keys = Vec::new();
        let mut c1_r_left_keys = Vec::new();
        let mut inner_join_c2_left_keys = Vec::new();
        let mut inner_join_c2_right_keys = Vec::new();
        for (lkey, rkey) in left_keys.iter().zip(right_keys.iter()) {
            let lcol = match &egraph[*lkey].nodes[0] {
                TokomakLogicalPlan::Column(c) => c,
                p => panic!("Found non-column value in inner join keys: {:?}", p),
            };
            let rcol = match &egraph[*rkey].nodes[0] {
                TokomakLogicalPlan::Column(c) => c,
                p => panic!("Found non-column value in inner join keys: {:?}", p),
            };
            assert!(
                col_from_plan(rschema, rcol),
                "Found right join key that was not in plan"
            );
            if col_from_plan(cross1_schema, lcol) {
                c1_r_left_keys.push(*lkey);
                c1_r_right_keys.push(*rkey);
            } else if col_from_plan(cross2_schema, lcol) {
                inner_join_c2_left_keys.push(*lkey);
                inner_join_c2_right_keys.push(*rkey);
            } else {
                return None;
            }
        }
        let c1_r_lkeys_id =
            egraph.add(TokomakLogicalPlan::EList(c1_r_left_keys.into_boxed_slice()));
        let c1_r_rkeys_id = egraph.add(TokomakLogicalPlan::EList(
            c1_r_right_keys.into_boxed_slice(),
        ));
        let c1_r_id =
            egraph.add(TokomakLogicalPlan::JoinKeys([c1_r_lkeys_id, c1_r_rkeys_id]));

        let inner_join_c2_lkeys_id = egraph.add(TokomakLogicalPlan::EList(
            inner_join_c2_left_keys.into_boxed_slice(),
        ));
        let inner_join_c2_rkeys_id = egraph.add(TokomakLogicalPlan::EList(
            inner_join_c2_right_keys.into_boxed_slice(),
        ));
        let inner_join_c2_id = egraph.add(TokomakLogicalPlan::JoinKeys([
            inner_join_c2_lkeys_id,
            inner_join_c2_rkeys_id,
        ]));

        subst.insert(inner_keys, c1_r_id);
        subst.insert(outer_keys, inner_join_c2_id);
        Some(())
    }
}

impl Tokomak {
    pub(crate) fn add_plan_simplification_rules(&mut self) {
        let rules = generate_plan_simplification_rules().unwrap();
        self.rules.extend(rules);
        info!("There are now {} rules", self.rules.len());
        self.added_builtins |= PLAN_SIMPLIFICATION_RULES;
    }

    pub(crate) fn add_filtered_plan_simplification_rules<
        F: Fn(&Rewrite<TokomakLogicalPlan, TokomakAnalysis>) -> bool,
    >(
        &mut self,
        filter: &F,
    ) {
        let rules = generate_plan_simplification_rules().unwrap();
        self.rules.extend(rules.into_iter().filter(|f| (filter)(f)));
        info!("There are now {} rules", self.rules.len());
        self.added_builtins |= PLAN_SIMPLIFICATION_RULES;
    }
}
