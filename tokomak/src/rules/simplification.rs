
use crate::{CustomTokomakAnalysis, SIMPLIFICATION_RULES, TokomakAnalysis, TokomakOptimizer};

use super::super::{ TokomakExpr};

use egg::{rewrite as rw, *};
#[allow(unused_variables)]
impl<T: CustomTokomakAnalysis> TokomakOptimizer<T>{
    #[allow(unused_variables, clippy::many_single_char_names)]
    pub(crate) fn add_simplification_rules(&mut self){
        let a: Var = "?a".parse().unwrap();
        let b: Var = "?b".parse().unwrap();
        let c: Var = "?c".parse().unwrap();
        let d: Var = "?d".parse().unwrap();
        let x: Var = "?x".parse().unwrap();
        //Add the one way rules first
        let mut rules = vec![
            rw!("commute-add"; "(+ ?x ?y)" => "(+ ?y ?x)"),
            rw!("commute-mul"; "(* ?x ?y)" => "(* ?y ?x)"),
            rw!("commute-and"; "(and ?x ?y)" => "(and ?y ?x)"),
            rw!("commute-or"; "(or ?x ?y)" => "(or ?y ?x)"),
            rw!("commute-eq"; "(= ?x ?y)" => "(= ?y ?x)"),
            rw!("commute-neq"; "(<> ?x ?y)" => "(<> ?y ?x)"),

            rw!("rotate-and"; "(and ?a (and ?b ?c))"=>"(and (and ?a ?b) ?c)"),
            rw!("rotate-or"; "(or ?a (or ?b ?c))"=>"(or (or ?a ?b) ?c)"),

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
        
            rw!("between_inverted-same"; "(between_inverted ?e ?a ?a)" => "(<> ?e ?a)" ),
            
            rw!("between_inverted-not-between"; "(between_inverted ?e ?a ?b)" => "(not (between ?e ?a ?b))"),
            //rw!("between-or-union"; "(or (between ?x ?a ?b) (between ?x ?c ?d))" => { BetweenMergeApplier{
            //    common_comparison: x,
            //    lhs_lower: a,
            //    lhs_upper: b,
            //    rhs_upper: d,
            //    rhs_lower: c,
            //}}),
        ];
        //Add the two way rules
        rules.extend(
        vec![
                rw!("expand-between"; "(between ?e ?a ?b)" <=> "(and (>= ?e ?a) (<= ?e ?b))"),
                rw!("expand-between_inverted"; "(between_inverted ?e ?a ?b)" <=> "(and (< ?e ?a) (> ?e ?b))"),
            ].into_iter().flatten()
        );
        self.rules.extend(rules);
        println!("There are now {} rules", self.rules.len());
        self.added_builtins |= SIMPLIFICATION_RULES;
    }
}


