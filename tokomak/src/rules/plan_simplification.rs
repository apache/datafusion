use egg::*;
use datafusion::error::DataFusionError;
use crate::{plan::TokomakLogicalPlan, Tokomak, PLAN_SIMPLIFICATION_RULES, pattern::{pattern, conditional_rule}, TokomakAnalysis};

use super::utils::*;

fn generate_simplification_rules()->Result<Vec<Rewrite<TokomakLogicalPlan, TokomakAnalysis>>, DataFusionError>{
    let predicate: Var = "?pred".parse().unwrap();
    let col: Var = "?col".parse().unwrap();
    let l:Var ="?l".parse().unwrap();
    let r:Var = "?r".parse().unwrap();
    let lcol :Var= "?lcol".parse().unwrap();
    let rcol :Var= "?rcol".parse().unwrap();
    
    let t1: Var = "?t1".parse().unwrap();
    let t2: Var = "?t2".parse().unwrap();
    let t3: Var = "?t3".parse().unwrap();
    let c1: Var = "?c1".parse().unwrap();
    let c2: Var = "?c2".parse().unwrap();
    let c3: Var = "?c3".parse().unwrap();
    let rules = vec![
        conditional_rule("plan-filter-crossjoin->innerjoin", 
            "(filter  \
                (cross_join ?l ?r ) \
                (and \
                    (= ?lcol ?rcol) \
                    ?pred \
                ) \
            )", "(filter (inner_join ?l ?r (keys ?lcol ?rcol)) ?pred)", and(column_from_plan(lcol, l), column_from_plan(rcol, r)) )?,
        //conditional_rule("filter-crossjoin-filter-predicate-pushdown", "(filter (cross_join ?l (filter ?r ?pred)) (and (= ?col ?other) ?other))", 
        //    "(filter (cross_join ?l (filter ?r (and ?pred (= ?col ?other))) (and (= ?col ?other) ?other))",
        //    column_from_plan(col, r)
        //)?,
        pattern("plan-remove-filter-true" ,"(filter ?input true)" ,"?input")?,
        pattern("plan-crossjoin-exchange", "(cross_join ?l ?r)", "(cross_join ?r ?l)")?,
        pattern("plan-merge-filters", "(filter (filter ?input ?inner_pred) ?outer_pred)", "(filter ?input (and ?inner_pred ?outer_pred))")?,
        //            pattern("expr-rotate-or", "(or ?a (or ?b ?c))","(or (or ?a ?b) ?c)")?,

        pattern("plan-crossjoin-rotate" ,"(cross_join ?t1 (cross_join ?t2 ?t3))", "(cross_join ?t1 (cross_join ?t3 ?t2))")?,
        //conditional_rule("plan-crossjoin-through", 
        //    "(filter (cross_join  ?t1 (cross_join ?t2 ?t3))  (and (= ?c1 ?c2) (and (= ?c2 ?c3) ?other)))",
        //    "(filter (inner_join ?t1 (inner_join ?t2 ?t3 (keys ?c2 ?c3)) (keys ?c1 ?c2)) ?other)",
        //    and(column_from_plan(c1, t1), and(column_from_plan(c2, t2), column_from_plan(c3, t3)))
        //)?
    ];
    Ok(rules)
}


impl Tokomak{
    pub(crate) fn add_plan_simplification_rules(&mut self){
        let rules = generate_simplification_rules().unwrap();
        self.rules.extend(rules);
        println!("There are now {} rules", self.rules.len());
        self.added_builtins |= PLAN_SIMPLIFICATION_RULES;
    }
}
