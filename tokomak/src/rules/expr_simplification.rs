
use crate::{ EXPR_SIMPLIFICATION_RULES, Tokomak, pattern::{pattern, twoway_pattern, transforming_pattern}, plan::TokomakLogicalPlan};

use super::{super::{ TokomakExpr}, utils::append};

use datafusion::error::DataFusionError;
use egg::{rewrite as rw, *};


fn generate_simplification_rules<A: Analysis<TokomakLogicalPlan> +'static>()->Result<Vec<Rewrite<TokomakLogicalPlan, A>>, DataFusionError>{
        let a: Var = "?a".parse().unwrap();
        let b: Var = "?b".parse().unwrap();
        let c: Var = "?c".parse().unwrap();
        let d: Var = "?d".parse().unwrap();
        let x: Var = "?x".parse().unwrap();
        let l: Var = "?l".parse().unwrap();
        let transformed: Var = "?transformed".parse().unwrap();
         //Add the one way rules first
        let mut rules = vec![
            //Arithmetic rules
            pattern("expr-commute-add", "(+ ?x ?y)", "(+ ?y ?x)")?,
            pattern("expr-commute-mul", "(* ?x ?y)", "(* ?y ?x)")?,
            pattern("expr-commute-and", "(and ?x ?y)", "(and ?y ?x)")?,
            pattern("expr-commute-or", "(or ?x ?y)", "(or ?y ?x)")?,
            pattern("expr-commute-eq", "(= ?x ?y)", "(= ?y ?x)")?,
            pattern("expr-commute-neq", "(<> ?x ?y)", "(<> ?y ?x)")?,
            
            //Expression tree rotating
            pattern("expr-rotate-add", "(+ ?x (+ ?y ?z))","(+ (+ ?x ?y) ?z)")?,
            pattern("expr-rotate-mul", "(* ?x (* ?y ?z))","(* (* ?x ?y) ?z)")?,            
            pattern("expr-rotate-and", "(and ?a (and ?b ?c))","(and (and ?a ?b) ?c)")?,
            pattern("expr-rotate-or", "(or ?a (or ?b ?c))","(or (or ?a ?b) ?c)")?,

            //Simplification

            pattern("expr-converse-gt", "(> ?x ?y)" , "(< ?y ?x)")?,
            pattern("expr-converse-gte", "(>= ?x ?y)" , "(<= ?y ?x)")?,
            pattern("expr-converse-lt", "(< ?x ?y)" , "(> ?y ?x)")?,
            pattern("expr-converse-lte", "(<= ?x ?y)" , "(>= ?y ?x)")?,
            pattern("expr-add-0", "(+ ?x 0)" , "?x")?,
            pattern("expr-add-assoc", "(+ (+ ?a ?b) ?c)" , "(+ ?a (+ ?b ?c))")?,
            pattern("expr-minus-0", "(- ?x 0)" , "?x")?,
            pattern("expr-mul-1", "(* ?x 1)" , "?x")?,
            pattern("expr-div-1", "(/ ?x 1)" , "?x")?,

            pattern("expr-dist-and-or", "(or (and ?a ?b) (and ?a ?c))" , "(and ?a (or ?b ?c))")?,
            pattern("expr-dist-or-and", "(and (or ?a ?b) (or ?a ?c))" , "(or ?a (and ?b ?c))")?,
            pattern("expr-not-not", "(not (not ?x))" , "?x")?,
            pattern("expr-or-same", "(or ?x ?x)" , "?x")?,
            pattern("expr-and-same", "(and ?x ?x)" , "?x")?,
            pattern("expr-and-true", "(and true ?x)", "?x")?,
            pattern("expr-0-minus", "(- 0 ?x)", "(negative ?x)")?,
            pattern("expr-and-false", "(and false ?x)", "false")?,
            pattern("expr-or-false", "(or false ?x)", "?x")?,
            pattern("expr-or-true", "(or true ?x)", "true")?,

            pattern("expr-or-to-inlist", "(or (= ?x ?a) (= ?x ?b))","(in_list ?x (elist ?a ?b))")?,
            transforming_pattern("expr-inlist-merge-or", "(or (= ?x ?a) (in_list ?x ?l))", "(in_list ?x ?transformed)", append(l, a), transformed)?,

            //("between-one-slice"; "(and (>= ?a ?b) (< ))","()")?,

            pattern("expr-between-same", "(between ?e ?a ?a)", "(= ?e ?a)")?,
        
            pattern("expr-between_inverted-same","(between_inverted ?e ?a ?a)" , "(<> ?e ?a)" )?,
            
            pattern("expr-between_inverted-not-between", "(between_inverted ?e ?a ?b)" , "(not (between ?e ?a ?b))")?,
            //rw!("between-or-union"; "(or (between ?x ?a ?b) (between ?x ?c ?d))" , { BetweenMergeApplier{
            //    common_comparison: x,
            //    lhs_lower: a,
            //    lhs_upper: b,
            //    rhs_upper: d,
            //    rhs_lower: c,
            //}})?,
        ];
        //Two way rules
        rules.extend(twoway_pattern("expr-expand-between", "(between ?e ?a ?b)", "(and (>= ?e ?a) (<= ?e ?b))")?);
        rules.extend(twoway_pattern("expr-expand-between_inverted", "(between_inverted ?e ?a ?b)","(and (< ?e ?a) (> ?e ?b))")?);
        Ok(rules)
    }
   

impl Tokomak{
    pub(crate) fn add_expr_simplification_rules(&mut self){
        let rules = generate_simplification_rules().unwrap();
        self.rules.extend(rules);
        println!("There are now {} rules", self.rules.len());
        self.added_builtins |= EXPR_SIMPLIFICATION_RULES;
    }
}


