
use crate::{error::DataFusionError};

use super::super::{Tokomak, TokomakExpr};

use egg::{rewrite as rw, *};
use super::utils::*;


pub fn add_simplification_rules(optimizer: &mut Tokomak){
    for rule in rules(){
        optimizer.add_rule(rule)
    }
}

fn rules()->Vec<Rewrite<TokomakExpr,()>>{
    let a:Var = "?a".parse().unwrap();
    let b: Var = "?b".parse().unwrap();
    let c: Var = "?c".parse().unwrap();
    let d: Var = "?d".parse().unwrap();
    let x: Var ="?x".parse().unwrap();
    vec![
        rw!("commute-add"; "(+ ?x ?y)" => "(+ ?y ?x)"),
        rw!("commute-mul"; "(* ?x ?y)" => "(* ?y ?x)"),
        rw!("commute-and"; "(and ?x ?y)" => "(and ?y ?x)"),
        rw!("commute-or"; "(or ?x ?y)" => "(or ?y ?x)"),
        rw!("commute-eq"; "(= ?x ?y)" => "(= ?y ?x)"),
        rw!("commute-neq"; "(<> ?x ?y)" => "(<> ?y ?x)"),
        rw!("converse-gt"; "(> ?x ?y)" => "(< ?y ?x)"),
        rw!("converse-gte"; "(>= ?x ?y)" => "(<= ?y ?x)"),
        rw!("converse-lt"; "(< ?x ?y)" => "(> ?y ?x)"),
        rw!("converse-lte"; "(<= ?x ?y)" => "(>= ?y ?x)"),
        rw!("add-0"; "(+ ?x 0)" => "?x"),
        rw!("add-assoc"; "(+ (+ ?a ?b) ?c)" => "(+ ?a (+ ?b ?c))"),
        rw!("minus-0"; "(- ?x 0)" => "?x"),
        rw!("mul-1"; "(* ?x 1)" => "?x"),
        rw!("div-1"; "(/ ?x 1)" => "?x"),
        rw!("dist-and-or"; "(or (and ?a ?b) (and ?a ?c))" => "(and ?a (or ?b ?c))"),
        rw!("dist-or-and"; "(and (or ?a ?b) (or ?a ?c))" => "(or ?a (and ?b ?c))"),
        rw!("not-not"; "(not (not ?x))" => "?x"),
        rw!("or-same"; "(or ?x ?x)" => "?x"),
        rw!("and-same"; "(and ?x ?x)" => "?x"),
        rw!("and-true"; "(and true ?x)"=> "?x"),
        rw!("0-minus"; "(- 0 ?x)"=> "(negative ?x)"),
        rw!("and-false"; "(and false ?x)"=> "false"),
        rw!("or-false"; "(or false ?x)"=> "?x"),
        rw!("or-true"; "(or true ?x)"=> "true"),
        rw!("between-same"; "(between ?e ?a ?a)"=> "(= ?e ?a)"),
        rw!("expand-between"; "(between ?e ?a ?b)" => "(and (>= ?e ?a) (<= ?e ?b))"),
        rw!("between_inverted-same"; "(between_inverted ?e ?a ?a)" => "(<> ?e ?a)" ),
        rw!("expand-between_inverted"; "(between_inverted ?e ?a ?b)" => "(and (< ?e ?a) (> ?e ?b))"),
        rw!("between_inverted-not-between"; "(between_inverted ?e ?a ?b)" => "(not (between ?e ?a ?b))"),
        rw!("between-or-union"; "(or (between ?x ?a ?b) (between ?x ?c ?d))" => { BetweenMergeApplier{
            common_comparison: x,
            lhs_lower: a,
            lhs_upper: b,
            rhs_upper: d,
            rhs_lower: c,
        }}),
    ]
}



struct BetweenMergeApplier{
    pub common_comparison: Var,
    pub lhs_lower: Var, 
    pub lhs_upper: Var, 
    pub rhs_lower: Var, 
    pub rhs_upper: Var
}

impl BetweenMergeApplier{
    fn try_merge(&self,egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst )->Result<(TokomakExpr, TokomakExpr), DataFusionError>{
        let lhs_low = convert_to_scalar_value(self.lhs_lower, egraph, id, subst)?;
        let lhs_high = convert_to_scalar_value(self.lhs_upper, egraph, id, subst)?;
        let rhs_low = convert_to_scalar_value(self.rhs_lower, egraph, id, subst)?;
        let rhs_high = convert_to_scalar_value(self.rhs_upper, egraph, id, subst)?;

        //Check if one is contained within another
        let rhs_high_in_lhs = gte(rhs_high.clone(), lhs_low.clone())? && lte(rhs_high.clone(), lhs_high.clone())?;
        let rhs_low_in_lhs = gte(rhs_low.clone(), lhs_low.clone())? && lte(rhs_low.clone(), lhs_high.clone())?;
        let is_overlap = rhs_high_in_lhs || rhs_low_in_lhs;
        if is_overlap{
            let new_lower = min(lhs_low, rhs_low)?;
            let new_high = max(lhs_high, rhs_high)?;
            return Ok((new_lower.into(),new_high.into()))
        }
        Err(DataFusionError::Internal(String::new()))
    }
}








impl Applier<TokomakExpr, ()> for BetweenMergeApplier{
    fn apply_one(&self, egraph: &mut EGraph<TokomakExpr, ()>, eclass: Id, subst: &Subst) -> Vec<Id> {
        let (lower, upper) = match self.try_merge(egraph, eclass, subst){
            Ok(new_range)=>new_range,
            Err(_) => return Vec::new(),
        };
        let lower_id = egraph.add(lower);
        let upper_id = egraph.add(upper);
        let common_compare = egraph[subst[self.common_comparison]].id;
        let new_between = TokomakExpr::Between([common_compare, lower_id, upper_id]);
        let new_between_id = egraph.add(new_between);
        vec![new_between_id]
    }
}



#[cfg(test)]
mod tests {

    use super::*;
    use egg::Runner;

    #[test]
    fn test_add_0() {
        let expr = "(+ 0 (x))".parse().unwrap();
        let runner = Runner::<TokomakExpr, (), ()>::default()
            .with_expr(&expr)
            .run(&rules());

        let mut extractor = Extractor::new(&runner.egraph, AstSize);

        let (_best_cost, best_expr) = extractor.find_best(runner.roots[0]);

        assert_eq!(format!("{}", best_expr), "x")
    }

    #[test]
    fn test_dist_and_or() {
        let expr = "(or (or (and (= 1 2) ?foo) (and (= 1 2) ?bar)) (and (= 1 2) ?boo))"
            .parse()
            .unwrap();
        let runner = Runner::<TokomakExpr, (), ()>::default()
            .with_expr(&expr)
            .run(&rules());

        let mut extractor = Extractor::new(&runner.egraph, AstSize);

        let (_, best_expr) = extractor.find_best(runner.roots[0]);

        assert_eq!(
            format!("{}", best_expr),
            "(and (= 1 2) (or ?boo (or ?foo ?bar)))"
        )
    }
}