use super::super::TokomakDataType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::Expr;
use datafusion::optimizer::utils::ConstEvaluator;
use datafusion::physical_plan::expressions::BinaryExpr;
use datafusion::physical_plan::type_coercion::data_types;
use datafusion::{
    error::DataFusionError,
    logical_plan::Operator,
    physical_plan::{
        expressions::{CastExpr, TryCastExpr},
        functions::{BuiltinScalarFunction, Volatility, Signature},
        ColumnarValue,
    },
    scalar::ScalarValue,
};
use crate::{CONST_FOLDING_RULES, CustomTokomakAnalysis, TokomakAnalysis, TokomakOptimizer};
use crate::{scalar::TokomakScalar, Tokomak, TokomakExpr};
use datafusion::arrow::datatypes::DataType;
use std::convert::TryInto;
use std::rc::Rc;

use super::utils::*;
use egg::{rewrite as rw, *};
use egg::Pattern;


impl<T: CustomTokomakAnalysis> TokomakOptimizer<T>{
    pub(crate) fn add_constant_folding_rules(&mut self){
        let a: Var = "?a".parse().unwrap();
        let b: Var = "?b".parse().unwrap();
        //let c: Var = "?c".parse().unwrap();
        //let d: Var = "?d".parse().unwrap();
        //let x: Var ="?x".parse().unwrap();
        let func: Var = "?func".parse().unwrap();
        let args: Var = "?args".parse().unwrap();
        
        let rules: Vec<Rewrite<TokomakExpr, TokomakAnalysis<T>>> = vec![
            //Move these 4 to simplification
            //rw!("const-prop-between"; "(and (>= ?e ?a) (<= ?e ?b))"=> "false" if var_gt(a,b)),
            //rw!("const-prop-between_inverted"; "(and (< ?e ?a)(> ?e ?b))" => "true" if var_gt(a,b)),
            //rw!("const-prop-binop-col-eq"; "(= ?a ?a)" => "(is_not_null ?a)"),
            //rw!("const-prop-binop-col-neq"; "(<> ?a ?a)" => "false"),
            //rw!("const-prop-add"; "(+ ?a ?b)"=>{ const_binop(a,b, Operator::Plus) } ),
            //rw!("const-prop-sub"; "(- ?a ?b)" => { const_binop(a, b, Operator::Minus)}),
            //rw!("const-prop-mul"; "(* ?a ?b)" => { const_binop(a,b, Operator::Multiply) } ),
            //rw!("const-prop-div"; "(/ ?a ?b)" =>{  const_binop(a,b, Operator::Divide)} ),
            //rw!("const-prop-mod"; "(% ?a ?b)" =>{const_binop(a,b, Operator::Modulo)}),
            //rw!("const-prop-or"; "(or ?a ?b)" =>{const_binop(a,b, Operator::Or)}),
            //rw!("const-prop-and"; "(and ?a ?b)" =>{const_binop(a,b, Operator::And)}),
            //rw!("const-prop-binop-eq"; "(= ?a ?b)"=>{ const_binop(a,b, Operator::Eq)} ),
            //rw!("const-prop-binop-neq"; "(<> ?a ?b)"=>{ const_binop(a,b, Operator::NotEq)}),
            //rw!("const-prop-binop-gt"; "(> ?a ?b)"=>{ const_binop(a,b, Operator::Gt)} ),
            //rw!("const-prop-binop-gte"; "(>= ?a ?b)"=>{ const_binop(a,b, Operator::GtEq)} ),
            //rw!("const-prop-binop-lt"; "(< ?a ?b)"=>{ const_binop(a,b, Operator::Lt)} ),
            //rw!("const-prop-binop-lte"; "(<= ?a ?b)"=>{ const_binop(a,b, Operator::LtEq)} ),
            //rw!("const-prop-regex_match"; "(regex_match ?a ?b)"=>{ const_binop(a,b, Operator::RegexMatch)} ),
            //rw!("const-prop-regex_imatch"; "(regex_imatch ?a ?b)"=>{ const_binop(a,b, Operator::RegexIMatch)} ),
            //rw!("const-prop-regex_not_match"; "(regex_not_match ?a ?b)"=>{ const_binop(a,b, Operator::RegexNotMatch)} ),
            //rw!("const-prop-regex_not_imatch"; "(regex_not_imatch ?a ?b)"=>{ const_binop(a,b, Operator::RegexNotIMatch)} ),
            //rw!("const-prop-like"; "(like ?a ?b)"=>{ const_binop(a,b, Operator::Like)} ),
            //rw!("const-prop-not_like"; "(not_like ?a ?b)"=>{ const_binop(a,b, Operator::NotLike)} ),
            //rw!("const-prop-cast"; "(cast ?a ?b)" =>{ ConstCastApplier{value: a, cast_type: b}} ),
            //rw!("const-prop-try_cast"; "(try_cast ?a ?b)" => {ConstTryCastApplier{value: a, cast_type: b}}),
            ////rw!("const-prop-call-scalar"; "(call ?func ?args)"=>{ ConstCallApplier{func, args}} if is_immutable_scalar_builtin_function(func)),
        ];
        self.rules.extend(rules);
        self.added_builtins |= CONST_FOLDING_RULES;
    }
    
}



fn lit_vars_and_gt<A: CustomTokomakAnalysis>(lhs: Var, rhs: Var, evaluator: Rc<ConstEvaluator>)->impl Fn(&mut EGraph<TokomakExpr, TokomakAnalysis<A>>, Id, &Subst)->bool{
    move |egraph: &mut EGraph<TokomakExpr,TokomakAnalysis<A>>, eclass: Id, subst: &Subst|->bool{
        let lhs_lit = &egraph[subst[lhs]].data.tokomak.const_folding;
        let rhs_lit = &egraph[subst[rhs]].data.tokomak.const_folding;
        match(lhs_lit,rhs_lit){
            (Some(lhs), Some(rhs)) => {
                let lhs=  Expr::Literal(lhs.into());
                let rhs = Expr::Literal(rhs.into());
                let expr = Expr::BinaryExpr{left: Box::new(lhs), op:Operator::Gt, right: Box::new(rhs)};
                let res = match evaluator.evaluate_to_scalar(expr){
                    Ok(s) => s,
                    Err(_) => return false,
                };
                match res{
                    ScalarValue::Boolean(Some(v)) => v,
                    _ => false,
                }
            }
            _ => false,
        }
    }
}



