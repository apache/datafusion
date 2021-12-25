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

//! Contains rules for expression simplification
use crate::{
    pattern::{pattern, transforming_pattern, twoway_pattern},
    plan::TokomakLogicalPlan,
    Tokomak, TokomakAnalysis, EXPR_SIMPLIFICATION_RULES,
};

use super::utils::*;
use datafusion::error::DataFusionError;
use egg::*;
use log::info;

pub(crate) fn generate_expr_simplification_rules<
    A: Analysis<TokomakLogicalPlan> + 'static,
>() -> Result<Vec<Rewrite<TokomakLogicalPlan, A>>, DataFusionError> {
    let a: Var = "?a".parse().unwrap();
    let x: Var = "?x".parse().unwrap();
    let y: Var = "?y".parse().unwrap();
    let z: Var = "?z".parse().unwrap();
    let r: Var = "?r".parse().unwrap();
    let l: Var = "?l".parse().unwrap();
    let transformed: Var = "?transformed".parse().unwrap();
    //Add the one way rules first
    let mut rules = vec![
        //Arithmetic rules
        Commute::rewrite(
            "expr-commute-add",
            "(+ ?l ?r)",
            l,
            r,
            TokomakLogicalPlan::Plus,
        )?,
        Commute::rewrite(
            "expr-commute-mul",
            "(* ?l ?r)",
            l,
            r,
            TokomakLogicalPlan::Multiply,
        )?,
        Commute::rewrite(
            "expr-commute-and",
            "(and ?l ?r)",
            l,
            r,
            TokomakLogicalPlan::And,
        )?,
        Commute::rewrite(
            "expr-commute-or",
            "(or ?l ?r)",
            l,
            r,
            TokomakLogicalPlan::Or,
        )?,
        Commute::rewrite("expr-commute-eq", "(= ?l ?r)", l, r, TokomakLogicalPlan::Eq)?,
        Commute::rewrite(
            "expr-commute-neq",
            "(<> ?l ?r)",
            l,
            r,
            TokomakLogicalPlan::NotEq,
        )?,
        //Expression tree rotating
        // "(+ ?x (+ ?y ?z))" =>"(+ (+ ?x ?y) ?z)"
        // Also commutes all off the binops
        CommutingRotate::rewrite(
            "expr-rotate-add",
            "(+ ?x (+ ?y ?z))",
            x,
            y,
            z,
            TokomakLogicalPlan::Plus,
        )?,
        CommutingRotate::rewrite(
            "expr-rotate-mul",
            "(* ?x (* ?y ?z))",
            x,
            y,
            z,
            TokomakLogicalPlan::Multiply,
        )?,
        CommutingRotate::rewrite(
            "expr-rotate-and",
            "(and ?x (and ?y ?z))",
            x,
            y,
            z,
            TokomakLogicalPlan::And,
        )?,
        CommutingRotate::rewrite(
            "expr-rotate-or",
            "(or ?x (or ?y ?z))",
            x,
            y,
            z,
            TokomakLogicalPlan::Or,
        )?,
        //Simplification
        pattern("expr-converse-gt", "(> ?x ?y)", "(< ?y ?x)")?,
        pattern("expr-converse-gte", "(>= ?x ?y)", "(<= ?y ?x)")?,
        pattern("expr-converse-lt", "(< ?x ?y)", "(> ?y ?x)")?,
        pattern("expr-converse-lte", "(<= ?x ?y)", "(>= ?y ?x)")?,
        pattern("expr-add-0", "(+ ?x 0)", "?x")?,
        pattern("expr-add-assoc", "(+ (+ ?a ?b) ?c)", "(+ ?a (+ ?b ?c))")?,
        pattern("expr-minus-0", "(- ?x 0)", "?x")?,
        pattern("expr-mul-1", "(* ?x 1)", "?x")?,
        pattern("expr-div-1", "(/ ?x 1)", "?x")?,
        pattern(
            "expr-dist-and-or",
            "(or (and ?a ?b) (and ?a ?c))",
            "(and ?a (or ?b ?c))",
        )?,
        pattern(
            "expr-dist-or-and",
            "(and (or ?a ?b) (or ?a ?c))",
            "(or ?a (and ?b ?c))",
        )?,
        pattern("expr-not-not", "(not (not ?x))", "?x")?,
        pattern("expr-or-same", "(or ?x ?x)", "?x")?,
        pattern("expr-and-same", "(and ?x ?x)", "?x")?,
        pattern("expr-and-true", "(and true ?x)", "?x")?,
        pattern("expr-0-minus", "(- 0 ?x)", "(negative ?x)")?,
        pattern("expr-or-false", "(or false ?x)", "?x")?,
        pattern(
            "expr-or-to-inlist",
            "(or (= ?x ?a) (= ?x ?b))",
            "(in_list ?x (elist ?a ?b))",
        )?,
        transforming_pattern(
            "expr-inlist-merge-or",
            "(or (= ?x ?a) (in_list ?x ?l))",
            "(in_list ?x ?transformed)",
            append_in_list(l, a, transformed),
            &[transformed],
        )?,
        pattern("expr-between-same", "(between ?e ?a ?a)", "(= ?e ?a)")?,
        pattern(
            "expr-between_inverted-same",
            "(between_inverted ?e ?a ?a)",
            "(<> ?e ?a)",
        )?,
        pattern(
            "expr-between_inverted-not-between",
            "(between_inverted ?e ?a ?b)",
            "(not (between ?e ?a ?b))",
        )?,
    ];
    //Two way rules
    rules.extend(twoway_pattern(
        "expr-expand-between",
        "(between ?e ?a ?b)",
        "(and (>= ?e ?a) (<= ?e ?b))",
    )?);
    rules.extend(twoway_pattern(
        "expr-expand-between_inverted",
        "(between_inverted ?e ?a ?b)",
        "(and (< ?e ?a) (> ?e ?b))",
    )?);
    Ok(rules)
}

struct CommutingRotate<F: Send + Sync + 'static + Fn([Id; 2]) -> TokomakLogicalPlan> {
    x: Var,
    y: Var,
    z: Var,
    f: F,
}

impl<F: Send + Sync + 'static + Fn([Id; 2]) -> TokomakLogicalPlan> CommutingRotate<F> {
    fn rewrite<A: Analysis<TokomakLogicalPlan>>(
        name: &str,
        searcher: &str,
        x: Var,
        y: Var,
        z: Var,
        f: F,
    ) -> Result<Rewrite<TokomakLogicalPlan, A>, DataFusionError> {
        use std::str::FromStr;
        let patt = egg::Pattern::from_str(searcher).map_err(|e| {
            DataFusionError::Plan(format!(
                "Could not create searcher for {}: {}",
                name, e
            ))
        })?;
        Rewrite::new(name, patt, CommutingRotate { x, y, z, f }).map_err(|e| {
            DataFusionError::Plan(format!("Could not create rewrite for {}: {}", name, e))
        })
    }
}
impl<
        A: Analysis<TokomakLogicalPlan>,
        F: Send + Sync + 'static + Fn([Id; 2]) -> TokomakLogicalPlan,
    > Applier<TokomakLogicalPlan, A> for CommutingRotate<F>
{
    fn apply_one(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        eclass: Id,
        subst: &Subst,
        _searcher_ast: Option<&PatternAst<TokomakLogicalPlan>>,
        _rule_name: Symbol,
    ) -> Vec<Id> {
        // "(+ ?x (+ ?y ?z))" =>"(+ (+ ?x ?y) ?z)"
        let inner = (self.f)([subst[self.x], subst[self.y]]); // (+ ?x ?y)
        let inner_id = egraph.add(inner);
        let outer = (self.f)([inner_id, subst[self.z]]); //(+ (+ ?x ?y) ?z)
        let outer_id = egraph.add(outer);
        if egraph.union(eclass, outer_id) {
            vec![eclass]
        } else {
            vec![]
        }
    }

    fn apply_matches(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        matches: &[SearchMatches<TokomakLogicalPlan>],
        _rule_name: Symbol,
    ) -> Vec<Id> {
        let mut added = vec![];
        for mat in matches {
            for subst in &mat.substs {
                let x = subst[self.x];
                let y = subst[self.y];
                let z = subst[self.z];
                let inner = (self.f)([x, y]); // (+ ?x ?y)

                let inner_commute = (self.f)([y, x]);
                let inner_commute_id = egraph.add(inner_commute);
                let inner_id = egraph.add(inner);
                egraph.union(inner_id, inner_commute_id);

                let outer = (self.f)([inner_id, z]); //(+ (+ ?x ?y) ?z)
                let outer_commute = (self.f)([z, inner_id]);
                let outer_commute_id = egraph.add(outer_commute);
                let outer_id = egraph.add(outer);
                egraph.union(outer_id, outer_commute_id);
                if egraph.union(mat.eclass, outer_id) {
                    added.push(mat.eclass);
                }
            }
        }
        added
    }
}

struct Commute<F: Send + Sync + 'static + Fn([Id; 2]) -> TokomakLogicalPlan> {
    l: Var,
    r: Var,
    f: F,
}
impl<F: Send + Sync + 'static + Fn([Id; 2]) -> TokomakLogicalPlan> Commute<F> {
    fn rewrite<A: Analysis<TokomakLogicalPlan>>(
        name: &str,
        searcher: &str,
        l: Var,
        r: Var,
        f: F,
    ) -> Result<Rewrite<TokomakLogicalPlan, A>, DataFusionError> {
        use std::str::FromStr;
        let patt = egg::Pattern::from_str(searcher).map_err(|e| {
            DataFusionError::Plan(format!(
                "Could not create searcher for {}: {}",
                name, e
            ))
        })?;
        Rewrite::new(name, patt, Commute { l, r, f }).map_err(|e| {
            DataFusionError::Plan(format!("Could not create rewrite for {}: {}", name, e))
        })
    }
}

impl<
        A: Analysis<TokomakLogicalPlan>,
        F: Send + Sync + 'static + Fn([Id; 2]) -> TokomakLogicalPlan,
    > Applier<TokomakLogicalPlan, A> for Commute<F>
{
    fn apply_one(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        eclass: Id,
        subst: &Subst,
        _searcher_ast: Option<&PatternAst<TokomakLogicalPlan>>,
        _rule_name: Symbol,
    ) -> Vec<Id> {
        let node = (self.f)([subst[self.r], subst[self.l]]);
        let id = egraph.add(node);
        if egraph.union(eclass, id) {
            vec![eclass]
        } else {
            vec![]
        }
    }

    fn apply_matches(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        matches: &[SearchMatches<TokomakLogicalPlan>],
        _rule_name: Symbol,
    ) -> Vec<Id> {
        let mut added = vec![];
        for mat in matches {
            for subst in &mat.substs {
                let node = (self.f)([subst[self.r], subst[self.l]]);
                let id = egraph.add(node);
                if egraph.union(mat.eclass, id) {
                    added.push(mat.eclass);
                }
            }
        }
        added
    }
}

impl Tokomak {
    pub(crate) fn add_filtered_expr_simplification_rules<
        F: Fn(&Rewrite<TokomakLogicalPlan, TokomakAnalysis>) -> bool,
    >(
        &mut self,
        f: &F,
    ) {
        let rules = generate_expr_simplification_rules().unwrap();
        self.rules.extend(rules.into_iter().filter(|r| (f)(r)));
        info!("There are now {} rules", self.rules.len());
        self.added_builtins |= EXPR_SIMPLIFICATION_RULES;
    }

    pub(crate) fn add_expr_simplification_rules(&mut self) {
        let rules = generate_expr_simplification_rules().unwrap();
        self.rules.extend(rules);
        info!("There are now {} rules", self.rules.len());
        self.added_builtins |= EXPR_SIMPLIFICATION_RULES;
    }
}
