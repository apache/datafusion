
use crate::{ EXPR_SIMPLIFICATION_RULES, Tokomak, pattern::{pattern, twoway_pattern, transforming_pattern}, plan::TokomakLogicalPlan};


use datafusion::error::DataFusionError;
use egg::{rewrite as rw, *};
use super::utils::*;

fn generate_simplification_rules<A: Analysis<TokomakLogicalPlan> +'static>()->Result<Vec<Rewrite<TokomakLogicalPlan, A>>, DataFusionError>{
        let a: Var = "?a".parse().unwrap();
        //let b: Var = "?b".parse().unwrap();
        //let c: Var = "?c".parse().unwrap();
        //let d: Var = "?d".parse().unwrap();
        let x: Var = "?x".parse().unwrap();
        let y: Var = "?y".parse().unwrap();
        let z: Var = "?z".parse().unwrap();
        let r: Var = "?r".parse().unwrap();
        let l: Var = "?l".parse().unwrap();
        let transformed: Var = "?transformed".parse().unwrap();
         //Add the one way rules first
        let mut rules = vec![
            //Arithmetic rules
            Commute::rewrite("expr-commute-add", "(+ ?l ?r)", l,r, TokomakLogicalPlan::Plus).unwrap(),
            Commute::rewrite("expr-commute-mul", "(* ?l ?r)", l,r, TokomakLogicalPlan::Multiply).unwrap(),
            Commute::rewrite("expr-commute-and", "(and ?l ?r)", l,r, TokomakLogicalPlan::And).unwrap(),
            Commute::rewrite("expr-commute-or", "(or ?l ?r)",l,r, TokomakLogicalPlan::Or).unwrap(),
            Commute::rewrite("expr-commute-eq", "(= ?l ?r)", l,r, TokomakLogicalPlan::Eq).unwrap(),
            Commute::rewrite("expr-commute-neq", "(<> ?l ?r)", l,r, TokomakLogicalPlan::NotEq).unwrap(),
            
            //Expression tree rotating
            // "(+ ?x (+ ?y ?z))" =>"(+ (+ ?x ?y) ?z)"
            Rotate::rewrite("expr-rotate-add", "(+ ?x (+ ?y ?z))",x,y,z, TokomakLogicalPlan::Plus).unwrap(),
            Rotate::rewrite("expr-rotate-mul", "(* ?x (* ?y ?z))",x,y,z, TokomakLogicalPlan::Multiply).unwrap(),            
            Rotate::rewrite("expr-rotate-and", "(and ?x (and ?y ?z))",x,y,z, TokomakLogicalPlan::And).unwrap(),
            Rotate::rewrite("expr-rotate-or", "(or ?x (or ?y ?z))",x,y,z, TokomakLogicalPlan::Or).unwrap(),

            //Simplification

            pattern("expr-converse-gt", "(> ?x ?y)" , "(< ?y ?x)").unwrap(),
            pattern("expr-converse-gte", "(>= ?x ?y)" , "(<= ?y ?x)").unwrap(),
            pattern("expr-converse-lt", "(< ?x ?y)" , "(> ?y ?x)").unwrap(),
            pattern("expr-converse-lte", "(<= ?x ?y)" , "(>= ?y ?x)").unwrap(),
            pattern("expr-add-0", "(+ ?x 0)" , "?x").unwrap(),
            pattern("expr-add-assoc", "(+ (+ ?a ?b) ?c)" , "(+ ?a (+ ?b ?c))").unwrap(),
            pattern("expr-minus-0", "(- ?x 0)" , "?x").unwrap(),
            pattern("expr-mul-1", "(* ?x 1)" , "?x").unwrap(),
            pattern("expr-div-1", "(/ ?x 1)" , "?x").unwrap(),

            pattern("expr-dist-and-or", "(or (and ?a ?b) (and ?a ?c))" , "(and ?a (or ?b ?c))").unwrap(),
            pattern("expr-dist-or-and", "(and (or ?a ?b) (or ?a ?c))" , "(or ?a (and ?b ?c))").unwrap(),
            pattern("expr-not-not", "(not (not ?x))" , "?x").unwrap(),
            pattern("expr-or-same", "(or ?x ?x)" , "?x").unwrap(),
            pattern("expr-and-same", "(and ?x ?x)" , "?x").unwrap(),
            pattern("expr-and-true", "(and true ?x)", "?x").unwrap(),
            pattern("expr-0-minus", "(- 0 ?x)", "(negative ?x)").unwrap(),
            pattern("expr-and-false", "(and false ?x)", "false").unwrap(),
            pattern("expr-or-false", "(or false ?x)", "?x").unwrap(),
            pattern("expr-or-true", "(or true ?x)", "true").unwrap(),
            pattern("expr-or-to-inlist", "(or (= ?x ?a) (= ?x ?b))","(in_list ?x (elist ?a ?b))").unwrap(),
            transforming_pattern("expr-inlist-merge-or", "(or (= ?x ?a) (in_list ?x ?l))", "(in_list ?x ?transformed)", append_in_list(l, a, transformed), &[transformed]).unwrap(),

            //("between-one-slice"; "(and (>= ?a ?b) (< ))","()").unwrap(),

            pattern("expr-between-same", "(between ?e ?a ?a)", "(= ?e ?a)").unwrap(),
        
            pattern("expr-between_inverted-same","(between_inverted ?e ?a ?a)" , "(<> ?e ?a)" ).unwrap(),
            
            pattern("expr-between_inverted-not-between", "(between_inverted ?e ?a ?b)" , "(not (between ?e ?a ?b))").unwrap(),
            //rw!("between-or-union"; "(or (between ?x ?a ?b) (between ?x ?c ?d))" , { BetweenMergeApplier{
            //    common_comparison: x,
            //    lhs_lower: a,
            //    lhs_upper: b,
            //    rhs_upper: d,
            //    rhs_lower: c,
            //}}).unwrap(),
        ];
        //Two way rules
        rules.extend(twoway_pattern("expr-expand-between", "(between ?e ?a ?b)", "(and (>= ?e ?a) (<= ?e ?b))").unwrap());
        rules.extend(twoway_pattern("expr-expand-between_inverted", "(between_inverted ?e ?a ?b)","(and (< ?e ?a) (> ?e ?b))").unwrap());
        Ok(rules)
    }
   

struct Rotate<F: Send + Sync + 'static + Fn([Id;2])->TokomakLogicalPlan>{
   x: Var,
   y: Var,
   z: Var,
   f: F
}

impl<F: Send + Sync + 'static + Fn([Id;2])->TokomakLogicalPlan> Rotate<F>{
    fn rewrite<A: Analysis<TokomakLogicalPlan>>(name: &str, searcher: &str, x: Var, y: Var,z: Var, f: F)->Result<Rewrite<TokomakLogicalPlan, A>, DataFusionError>{
        use std::str::FromStr;
        let patt = egg::Pattern::from_str(searcher).map_err(|e| DataFusionError::Plan(format!("Could not create searcher for {}: {}", name , e)))?;
        Rewrite::new(name, patt, Rotate{x,y,z,f}).map_err(|e| DataFusionError::Plan(format!("Could not create rewrite for {}: {}", name , e)))
    }
}
impl<A: Analysis<TokomakLogicalPlan>, F: Send + Sync + 'static +  Fn([Id;2])->TokomakLogicalPlan> Applier<TokomakLogicalPlan, A> for Rotate<F>{
    fn apply_one(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&PatternAst<TokomakLogicalPlan>>,
        rule_name: Symbol,
    ) -> Vec<Id> {
        // "(+ ?x (+ ?y ?z))" =>"(+ (+ ?x ?y) ?z)"
        let inner = (self.f)([subst[self.x], subst[self.y]]);// (+ ?x ?y)
        let inner_id = egraph.add(inner);
        let outer = (self.f)([inner_id, subst[self.z]]);//(+ (+ ?x ?y) ?z)
        let outer_id = egraph.add(outer);
        if egraph.union(eclass, outer_id){
            vec![eclass]
        }else{
            vec![]
        }
    }

    fn apply_matches(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        matches: &[SearchMatches<TokomakLogicalPlan>],
        rule_name: Symbol,
    ) -> Vec<Id> {
        let mut added = vec![];
        for mat in matches {
            for subst in &mat.substs {
                let x = subst[self.x];
                let y = subst[self.y];
                let z = subst[self.z];
                let inner = (self.f)([x, y]);// (+ ?x ?y)

                let inner_commute = (self.f)([y,x]);
                let inner_commute_id = egraph.add(inner_commute);
                let inner_id = egraph.add(inner);
                egraph.union(inner_id, inner_commute_id);

                let outer = (self.f)([inner_id, z]);//(+ (+ ?x ?y) ?z)
                let outer_commute = (self.f)([z, inner_id]);
                let outer_commute_id = egraph.add(outer_commute);
                let outer_id = egraph.add(outer);
                egraph.union(outer_id, outer_commute_id);
                if egraph.union(mat.eclass, outer_id){
                    added.push(mat.eclass);
                }
            }
        }
        added
    }

}

struct Commute<F:Send + Sync + 'static+ Fn([Id;2])->TokomakLogicalPlan>{
    l: Var,
    r: Var,
    f: F
}
impl<F: Send + Sync + 'static + Fn([Id;2])->TokomakLogicalPlan> Commute<F>{
    fn rewrite<A: Analysis<TokomakLogicalPlan>>(name: &str, searcher: &str, l: Var, r: Var, f: F)->Result<Rewrite<TokomakLogicalPlan, A>, DataFusionError>{
        use std::str::FromStr;
        let patt = egg::Pattern::from_str(searcher).map_err(|e| DataFusionError::Plan(format!("Could not create searcher for {}: {}", name , e)))?;
        Rewrite::new(name, patt, Commute{l,r,f}).map_err(|e| DataFusionError::Plan(format!("Could not create rewrite for {}: {}", name , e)))
    }
}

impl<A: Analysis<TokomakLogicalPlan>, F: Send + Sync + 'static +  Fn([Id;2])->TokomakLogicalPlan> Applier<TokomakLogicalPlan, A> for Commute<F>{
    fn apply_one(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&PatternAst<TokomakLogicalPlan>>,
        rule_name: Symbol,
    ) -> Vec<Id> {
        let node = (self.f)([subst[self.r],subst[self.l]]);
        let id = egraph.add(node);
        if egraph.union(eclass, id){
            vec![eclass]
        }else{
            vec![]
        }
    }

    fn apply_matches(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        matches: &[SearchMatches<TokomakLogicalPlan>],
        rule_name: Symbol,
    ) -> Vec<Id> {
        let mut added = vec![];
        for mat in matches {
            for subst in &mat.substs {
                let node = (self.f)([subst[self.r],subst[self.l]]);
                let id = egraph.add(node);
                if egraph.union(mat.eclass, id){
                    added.push(mat.eclass);
                }
            }
        }
        added
    }
}


impl Tokomak{
    pub(crate) fn add_expr_simplification_rules(&mut self){
        let rules = generate_simplification_rules().unwrap();
        self.rules.extend(rules);
        println!("There are now {} rules", self.rules.len());
        self.added_builtins |= EXPR_SIMPLIFICATION_RULES;
    }
}


