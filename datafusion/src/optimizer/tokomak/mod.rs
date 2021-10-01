#![allow(missing_docs)]

//!
//! This saves time in planning and executing the query.

use arrow::datatypes::DataType;
use std::convert::TryInto;
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;

use crate::physical_plan::udaf::AggregateUDF;
use crate::physical_plan::udf::ScalarUDF;
use crate::{logical_plan::LogicalPlan, optimizer::optimizer::OptimizerRule};
use log::debug;

use crate::error::{DataFusionError, Result as DFResult};
use crate::execution::context::ExecutionProps;
use crate::logical_plan::{Column, Expr};
use crate::{logical_plan::Operator, optimizer::utils};
use std::collections::HashMap;
use std::sync::Arc;

use egg::*;

pub mod datatype;
pub mod expr;
pub mod rules;
pub mod scalar;
use datatype::TokomakDataType;
use expr::TokomakExpr;

use std::rc::Rc;

pub trait UDFRegistry {
    fn add_sudf(&mut self, udf: Arc<ScalarUDF>);
    fn add_uadf(&mut self, udf: Arc<AggregateUDF>);
}

impl UDFRegistry for HashMap<String, UDF> {
    fn add_sudf(&mut self, udf: Arc<ScalarUDF>) {
        if !self.contains_key(&udf.name) {
            self.insert(udf.name.to_owned(), UDF::Scalar(udf));
        }
    }

    fn add_uadf(&mut self, udf: Arc<AggregateUDF>) {
        if !self.contains_key(&udf.name) {
            self.insert(udf.name.to_owned(), UDF::Aggregate(udf));
        }
    }
}
pub trait UDFAwareRuleGenerator {
    fn generate_rules(
        &self,
        udf_registry: Rc<HashMap<String, UDF>>,
    ) -> Vec<Rewrite<TokomakExpr, ()>>;
}
impl<F: Fn(Rc<HashMap<String, UDF>>) -> Vec<Rewrite<TokomakExpr, ()>>>
    UDFAwareRuleGenerator for F
{
    fn generate_rules(
        &self,
        udf_registry: Rc<HashMap<String, UDF>>,
    ) -> Vec<Rewrite<TokomakExpr, ()>> {
        self(udf_registry)
    }
}

pub struct Tokomak {
    rules: Vec<Rewrite<TokomakExpr, ()>>,
    udf_aware_rules: Vec<Box<dyn UDFAwareRuleGenerator>>,
}
impl Tokomak {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            udf_aware_rules: Vec::new(),
        }
    }
    pub fn with_rules(custom_rules: &[Rewrite<TokomakExpr, ()>]) -> Self {
        let mut new = Self::new();
        new.add_rules(custom_rules);
        new
    }
    pub fn add_rules(&mut self, rules: &[Rewrite<TokomakExpr, ()>]) {
        self.rules.extend_from_slice(rules);
    }
    pub fn add_rule(&mut self, rule: Rewrite<TokomakExpr, ()>) {
        self.rules.push(rule);
    }
    pub fn add_udf_aware_rules(&mut self, generator: Box<dyn UDFAwareRuleGenerator>) {
        self.udf_aware_rules.push(generator);
    }
}

fn convert_to_tokomak_expr(
    expr: Expr,
    udf_reg: &mut HashMap<String, UDF>,
) -> DFResult<RecExpr<TokomakExpr>> {
    let mut rec_expr = RecExpr::default();
    let mut converter = ExprConverter::new(&mut rec_expr, udf_reg);
    converter.to_tokomak_expr(expr)?;
    Ok(rec_expr)
}

fn to_expr(
    rec_expr: &RecExpr<TokomakExpr>,
    udf_reg: &HashMap<String, UDF>,
) -> DFResult<Expr> {
    let converter = TokomakExprConverter::new(udf_reg, rec_expr);
    let start = rec_expr.as_ref().len() - 1;
    converter.convert_to_expr(start.into())
}
struct TokomakExprConverter<'a> {
    udf_reg: &'a HashMap<String, UDF>,
    rec_expr: &'a RecExpr<TokomakExpr>,
    refs: &'a [TokomakExpr],
}

impl<'a> TokomakExprConverter<'a> {
    fn new(
        udf_reg: &'a HashMap<String, UDF>,
        rec_expr: &'a RecExpr<TokomakExpr>,
    ) -> Self {
        Self {
            udf_reg,
            rec_expr,
            refs: rec_expr.as_ref(),
        }
    }
    fn get_ref(&self, id: Id) -> &TokomakExpr {
        let idx: usize = id.into();
        &self.refs[idx]
    }

    fn to_list(&self, id: Id) -> DFResult<Vec<Expr>> {
        let idx: usize = id.into();
        let list_expr = &self.rec_expr.as_ref()[idx];
        match list_expr {
            TokomakExpr::List(ids) => ids
                .iter()
                .map(|i| self.convert_to_expr(*i))
                .collect::<DFResult<Vec<_>>>(),
            e => Err(DataFusionError::Internal(format!(
                "Expected Tokomak list found: {:?}",
                e
            ))),
        }
    }
    fn to_binary_op(&self, &[l, r]: &[Id; 2], op: Operator) -> DFResult<Expr> {
        let l = self.convert_to_expr(l)?;
        let r = self.convert_to_expr(r)?;
        Ok(Expr::BinaryExpr {
            left: Box::new(l),
            op,
            right: Box::new(r),
        })
    }
    fn convert_to_expr(&self, id: Id) -> DFResult<Expr> {
        let expr = match self.get_ref(id) {
            TokomakExpr::Plus(ids) => self.to_binary_op(ids, Operator::Plus)?,
            TokomakExpr::Minus(ids) => self.to_binary_op(ids, Operator::Minus)?,
            TokomakExpr::Divide(ids) =>self.to_binary_op(ids, Operator::Divide)?,
            TokomakExpr::Modulus(ids) => self.to_binary_op(ids, Operator::Modulo)?,
            TokomakExpr::Multiply(ids) => self.to_binary_op(ids, Operator::Multiply)?,
            TokomakExpr::Or(ids) => self.to_binary_op(ids, Operator::Or)?,
            TokomakExpr::And(ids) => self.to_binary_op(ids, Operator::And)?,
            TokomakExpr::Eq(ids) => self.to_binary_op(ids, Operator::Eq)?,
            TokomakExpr::NotEq(ids) => self.to_binary_op(ids, Operator::NotEq)?,
            TokomakExpr::Lt(ids) => self.to_binary_op(ids, Operator::Lt)?,
            TokomakExpr::LtEq(ids) =>self.to_binary_op(ids, Operator::LtEq)?,
            TokomakExpr::Gt(ids) => self.to_binary_op(ids, Operator::Gt)?,
            TokomakExpr::GtEq(ids) => self.to_binary_op(ids, Operator::GtEq)?,
            TokomakExpr::Like(ids) => self.to_binary_op(ids, Operator::Like)?,
            TokomakExpr::NotLike(ids) => self.to_binary_op(ids, Operator::NotLike)?,
            TokomakExpr::RegexMatch(ids) => self.to_binary_op(ids, Operator::RegexMatch)?,
            TokomakExpr::RegexIMatch(ids) => self.to_binary_op(ids, Operator::RegexIMatch)?,
            TokomakExpr::RegexNotMatch(ids) => self.to_binary_op(ids, Operator::RegexNotMatch)?,
            TokomakExpr::RegexNotIMatch(ids) => self.to_binary_op(ids, Operator::RegexNotIMatch)?,
            TokomakExpr::Not(id) => {
                let l = self.convert_to_expr(*id)?;
                Expr::Not(Box::new(l))
            }
            TokomakExpr::IsNotNull(id) => {
                let l = self.convert_to_expr(*id)?;
                Expr::IsNotNull(Box::new(l))
            }
            TokomakExpr::IsNull(id) => {
                let l = self.convert_to_expr(*id)?;
                Expr::IsNull(Box::new(l))
            }
            TokomakExpr::Negative(id) => {
                let l = self.convert_to_expr(*id)?;
                Expr::Negative(Box::new(l))
            }
            TokomakExpr::Between([expr, low, high]) => {
                let left = self.convert_to_expr(*expr)?;
                let low_expr = self.convert_to_expr(*low)?;
                let high_expr = self.convert_to_expr(*high)?;
                Expr::Between {
                    expr: Box::new(left),
                    negated: false,
                    low: Box::new(low_expr),
                    high: Box::new(high_expr),
                }
            }
            TokomakExpr::BetweenInverted([expr, low, high]) => {
                let left = self.convert_to_expr(*expr)?;
                let low_expr = self.convert_to_expr(*low)?;
                let high_expr = self.convert_to_expr(*high)?;
                Expr::Between {
                    expr: Box::new(left),
                    negated: false,
                    low: Box::new(low_expr),
                    high: Box::new(high_expr),
                }
            }
            //TODO: Fix column handling
            TokomakExpr::Column(col) => Expr::Column(Column{name: col.to_string(), relation:None}),
            TokomakExpr::Cast([e, ty]) => {
                let l = self.convert_to_expr(*e)?;
                let dt = match self.get_ref(*ty) {
                    TokomakExpr::Type(s) => s,
                    e => return Err(DataFusionError::Internal(format!("Cast expected a type expression in the second position, found: {:?}",e))),
                };
                let dt: DataType = dt.into();
                Expr::Cast { expr: Box::new(l), data_type: dt}
            }
            TokomakExpr::Type(_) => {
                panic!("Type should only be part of expression")
            }
            TokomakExpr::InList([val, list])=>{
                let l = self.convert_to_expr(*val)?;
                let list = self.get_ref(*list);
                let list = match list{
                    TokomakExpr::List(ref l) => l.iter().map(|i| self.convert_to_expr(*i )).collect::<DFResult<Vec<Expr>>>()?,
                    e => return Err(DataFusionError::Internal(format!("InList expected a list in the second position found {:?}", e))),
                };
                Expr::InList{
                  list,
                  expr: Box::new(l),
                  negated: false,
                }
            }
            TokomakExpr::NotInList([val, list])=>{
                let l = self.convert_to_expr(*val)?;
                let list = self.get_ref(*list);
                let list = match list{
                    TokomakExpr::List(ref l) => l.iter().map(|i| self.convert_to_expr(*i )).collect::<DFResult<Vec<Expr>>>()?,
                    e=> return Err(DataFusionError::Internal(format!("NotInList expected a list in the second position found {:?}", e))),
                };
                Expr::InList{
                  list,
                  expr: Box::new(l),
                  negated: true,
                }
            }
            TokomakExpr::List(_) => return Err(DataFusionError::Internal("TokomakExpr::List should only ever be a child expr and should be handled by the parent expression".to_string())),
            TokomakExpr::Scalar(s) => Expr::Literal(s.clone().into()),
            TokomakExpr::ScalarBuiltin(_) => panic!("ScalarBuiltin should only be part of an expression"),
            TokomakExpr::ScalarBuiltinCall([fun, args]) => {
                let fun = match self.get_ref(*fun){
                    TokomakExpr::ScalarBuiltin(f)=>f,
                    f => return Err(DataFusionError::Internal(format!("Expected a builtin scalar function function in the first position, found {:?}", f))),
                };
                let arg_ids = match self.get_ref(*args){
                    TokomakExpr::List(args)=> args,
                    e => panic!("Expected a list of function arguments for a ScalarBuiltinCall, found: {:?}", e),
                };
                let args = arg_ids.iter().map(|expr| self.convert_to_expr(*expr)).collect::<DFResult<Vec<_>>>()?;
                Expr::ScalarFunction{
                    fun:fun.clone(),
                    args,
                }
            },
            TokomakExpr::TryCast([e, ty]) => {
                let l = self.convert_to_expr(*e)?;
                let dt = match self.get_ref(*ty) {
                    TokomakExpr::Type(s) => s,
                    e => return Err(DataFusionError::Internal(format!("Cast expected a type expression in the second position, found: {:?}",e))),
                };
                let dt: DataType = dt.into();
                Expr::TryCast { expr: Box::new(l), data_type: dt}
            },
            TokomakExpr::AggregateBuiltin(_) => todo!(),
            TokomakExpr::ScalarUDFCall([name, args]) => {
                let args = self.get_ref(*args);
                let args = match args{
                    TokomakExpr::List(ref l) => l.iter().map(|i| self.convert_to_expr(*i )).collect::<DFResult<Vec<Expr>>>()?,
                    e => return Err(DataFusionError::Internal(format!("ScalarUDFCall expected a type expression in the second position, found: {:?}",e))),
                };
                let name = self.get_ref(*name);
                let name = match name{
                    TokomakExpr::ScalarUDF(sym)=> sym.0.to_string(),
                    e => panic!("Found a non ScalarUDF node in the first position of ScalarUdf: {:#?}",e),
                };
                let fun = match self.udf_reg.get(&name){
                    Some(s) => s,
                    None => return Err(DataFusionError::Internal(format!("Did not find the scalar UDF {} in the registry", name))),
                };
                let fun = match fun{
                    UDF::Scalar(s) => s.clone(),
                    UDF::Aggregate(_) => return Err(DataFusionError::Internal(format!("Did not find scalar UDF named {}. Found an aggregate UDF instead.", name))),
                };
                Expr::ScalarUDF{
                    fun,
                    args
                }
            },
            TokomakExpr::AggregateBuiltinCall([fun, args]) =>{
                let fun = match self.get_ref(*fun){
                    TokomakExpr::AggregateBuiltin(f)=>f.clone(),
                    e => return Err(DataFusionError::Internal(format!("Expected a built in AggregateFunction, found {:?}", e))),
                };
                let args = self.to_list(*args).map_err(|e| DataFusionError::Internal(format!("AggregateBuiltinCall could not convert args expr to list of expressions: {}", e)))?;
                Expr::AggregateFunction{
                    fun,
                    args,
                    distinct: false
                }
            },
            TokomakExpr::AggregateBuiltinDistinctCall([fun,args]) => {
                let fun = match self.get_ref(*fun){
                    TokomakExpr::AggregateBuiltin(f)=>f.clone(),
                    e => return Err(DataFusionError::Internal(format!("Expected a built in AggregateFunction, found {:?}", e))),
                };
                let args = self.to_list(*args).map_err(|e| DataFusionError::Internal(format!("AggregateBuiltinDistinctCall could not convert args expr to list of expressions: {}", e)))?;
                Expr::AggregateFunction{
                    fun,
                    args,
                    distinct: true
                }
            },
            TokomakExpr::AggregateUDF(name) => return Err(DataFusionError::Internal(format!("Encountered an AggregateUDF expression with the name {}, these should only occur in a AggregateUDFCall and should be dealt with there", name))),

            TokomakExpr::ScalarUDF(name) => return Err(DataFusionError::Internal(format!("Encountered an ScalarUDF expression with the name {}, these should only occur in a ScalarUDFCall and should be dealt with there", name))),
            TokomakExpr::AggregateUDFCall([fun, args]) => {
                let udf_name = match self.get_ref(*fun){
                    TokomakExpr::AggregateUDF(name) => name.to_string(),
                    e =>    return Err(DataFusionError::Internal(format!("Expected an AggregateUDF node at index 0 of AggregateUDFCall, found: {:?}",e)))
                };
                let fun = match self.udf_reg.get(&udf_name){
                    Some(s) => s,
                    None => return Err(DataFusionError::Internal(format!("Did not find the aggregate UDF '{}' in the registry", udf_name))),
                };
                let fun = match fun{
                    UDF::Aggregate(a) =>a.clone() ,
                    UDF::Scalar(_) => return Err(DataFusionError::Internal(format!("Did not find aggregate UDF named {}. Found a scalar UDF instead.", udf_name))),
                };
                let args = self.to_list(*args).map_err(|e| DataFusionError::Internal(format!("AggregateUDFCall could not convert args expr to list of expressions: {}", e)))?;
                Expr::AggregateUDF{
                    fun,
                    args
                }
            },
            TokomakExpr::WindowBuiltinCallUnframed([fun, partition, order , args]) => {
                let fun = match self.get_ref(*fun){
                    TokomakExpr::WindowBuiltin(f)=>f.clone(),
                    e=>return Err(DataFusionError::Internal(format!("WindowBuiltinCallUnframed expected a WindowBuiltin expression found {:?}",e))),
                };
                let partition_by = self.to_list(*partition).map_err(
                    |e| DataFusionError::Internal(format!("WindowBuiltinCallUnframed could not transform parition expressions: {:?}", e))
                )?;
                let order_by = self.to_list(*order).map_err(
                    |e| DataFusionError::Internal(format!("WindowBuiltinCallUnframed could not transform order expressions: {:?}", e))
                )?;
                let args = self.to_list(*args).map_err(
                    |e| DataFusionError::Internal(format!("WindowBuiltinCallUnframed could not transform the argument list: {:?}", e))
                )?;
                Expr::WindowFunction{
                    fun,
                    partition_by,
                    order_by,
                    args,
                    window_frame: None
                }
            },
            TokomakExpr::WindowBuiltinCallFramed([fun, partition, order, frame, args]) => {
                let fun = match self.get_ref(*fun){
                    TokomakExpr::WindowBuiltin(f)=>f.clone(),
                    e=>return Err(DataFusionError::Internal(format!("WindowBuiltinCallUnframed expected a WindowBuiltin expression found {:?}",e))),
                };
                let partition_by = self.to_list(*partition).map_err(
                    |e| DataFusionError::Internal(format!("WindowBuiltinCallUnframed could not transform parition expressions: {:?}", e))
                )?;
                let order_by = self.to_list(*order).map_err(
                    |e| DataFusionError::Internal(format!("WindowBuiltinCallUnframed could not transform order expressions: {:?}", e))
                )?;
                let args = self.to_list(*args).map_err(
                    |e| DataFusionError::Internal(format!("WindowBuiltinCallUnframed could not transform the argument list: {:?}", e))
                )?;
                let window_frame = match self.get_ref(*frame){
                    TokomakExpr::WindowFrame(f)=>f.clone(),
                    e=> return Err(DataFusionError::Internal(format!("WindowBuiltinCallFramed expected a WindowFrame expression, found: {:?}", e))),
                };
                Expr::WindowFunction{
                    fun,
                    partition_by,
                    order_by,
                    args,
                    window_frame: Some(window_frame)
                }
            },
            TokomakExpr::Sort([e, sort_spec]) => {
                let expr = self.convert_to_expr(*e)?;
                let sort_spec = match self.get_ref(*sort_spec){
                    TokomakExpr::SortSpec(s)=>s.clone(),
                    e => return Err(unexpected_tokomak_expr("Sort", "SortSpec", e)),
                };
                let (asc, nulls_first)= match sort_spec{
                    SortSpec::Asc => (true,false),
                    SortSpec::Desc => (false, false),
                    SortSpec::AscNullsFirst => (true, true),
                    SortSpec::DescNullsFirst => (false,true),
                };
                Expr::Sort{
                    expr: Box::new(expr),
                    asc,
                    nulls_first
                }
            },
            TokomakExpr::SortSpec(_) => todo!(),
            TokomakExpr::WindowFrame(_) => todo!(),
            TokomakExpr::WindowBuiltin(_) => todo!(),
        };
        Ok(expr)
    }
}

fn unexpected_tokomak_expr(
    name: &str,
    expected: &str,
    found: &TokomakExpr,
) -> DataFusionError {
    DataFusionError::Internal(format!(
        "{} expected a {} expression found: {:?}",
        name, expected, found
    ))
}

struct ExprConverter<'a> {
    rec_expr: &'a mut RecExpr<TokomakExpr>,
    udf_registry: &'a mut HashMap<String, UDF>,
}

impl<'a> ExprConverter<'a> {
    fn new(
        rec_expr: &'a mut RecExpr<TokomakExpr>,
        udf_registry: &'a mut HashMap<String, UDF>,
    ) -> Self {
        ExprConverter {
            rec_expr,
            udf_registry,
        }
    }

    fn add_list(&mut self, exprs: Vec<Expr>) -> Result<Id, DataFusionError> {
        let list = exprs
            .into_iter()
            .map(|expr| self.to_tokomak_expr(expr))
            .collect::<Result<Vec<Id>, _>>()?;
        Ok(self.rec_expr.add(TokomakExpr::List(list)))
    }

    fn to_tokomak_expr(&mut self, expr: Expr) -> Result<Id, DataFusionError> {
        Ok(match expr {
            Expr::BinaryExpr { left, op, right } => {
                let left = self.to_tokomak_expr(*left)?;
                let right = self.to_tokomak_expr(*right)?;
                let binary_expr = match op {
                    Operator::Eq => TokomakExpr::Eq,
                    Operator::NotEq => TokomakExpr::NotEq,
                    Operator::Lt => TokomakExpr::Lt,
                    Operator::LtEq => TokomakExpr::LtEq,
                    Operator::Gt => TokomakExpr::Gt,
                    Operator::GtEq => TokomakExpr::GtEq,
                    Operator::Plus => TokomakExpr::Plus,
                    Operator::Minus => TokomakExpr::Minus,
                    Operator::Multiply => TokomakExpr::Multiply,
                    Operator::Divide => TokomakExpr::Divide,
                    Operator::Modulo => TokomakExpr::Modulus,
                    Operator::And => TokomakExpr::And,
                    Operator::Or => TokomakExpr::Or,
                    Operator::Like => TokomakExpr::Like,
                    Operator::NotLike => TokomakExpr::NotLike,
                    Operator::RegexMatch => TokomakExpr::RegexMatch,
                    Operator::RegexIMatch => TokomakExpr::RegexIMatch,
                    Operator::RegexNotMatch => TokomakExpr::RegexNotMatch,
                    Operator::RegexNotIMatch => TokomakExpr::RegexNotIMatch,
                };
                self.rec_expr.add(binary_expr([left, right]))
            }
            Expr::Column(c) => self
                .rec_expr
                .add(TokomakExpr::Column(Symbol::from(c.flat_name()))),
            Expr::Literal(s) => self.rec_expr.add(TokomakExpr::Scalar(s.into())),
            Expr::Not(expr) => {
                let e = self.to_tokomak_expr(*expr)?;
                self.rec_expr.add(TokomakExpr::Not(e))
            }
            Expr::IsNull(expr) => {
                let e = self.to_tokomak_expr(*expr)?;
                self.rec_expr.add(TokomakExpr::IsNull(e))
            }
            Expr::IsNotNull(expr) => {
                let e = self.to_tokomak_expr(*expr)?;
                self.rec_expr.add(TokomakExpr::IsNotNull(e))
            }
            Expr::Negative(expr) => {
                let e = self.to_tokomak_expr(*expr)?;
                self.rec_expr.add(TokomakExpr::Negative(e))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let e = self.to_tokomak_expr(*expr)?;
                let low = self.to_tokomak_expr(*low)?;
                let high = self.to_tokomak_expr(*high)?;
                if negated {
                    self.rec_expr
                        .add(TokomakExpr::BetweenInverted([e, low, high]))
                } else {
                    self.rec_expr.add(TokomakExpr::Between([e, low, high]))
                }
            }

            Expr::Cast { expr, data_type } => {
                let ty = data_type.try_into()?;
                let e = self.to_tokomak_expr(*expr)?;
                let t = self.rec_expr.add(TokomakExpr::Type(ty));

                self.rec_expr.add(TokomakExpr::Cast([e, t]))
            }
            Expr::TryCast { expr, data_type } => {
                let ty: TokomakDataType = data_type.try_into()?;
                let e = self.to_tokomak_expr(*expr)?;
                let t = self.rec_expr.add(TokomakExpr::Type(ty));
                self.rec_expr.add(TokomakExpr::TryCast([e, t]))
            }
            Expr::ScalarFunction { fun, args } => {
                let fun_id = self.rec_expr.add(TokomakExpr::ScalarBuiltin(fun));
                let args_id = self.add_list(args)?;
                self.rec_expr
                    .add(TokomakExpr::ScalarBuiltinCall([fun_id, args_id]))
            }
            Expr::Alias(expr, _) => self.to_tokomak_expr(*expr)?,
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val_expr = self.to_tokomak_expr(*expr)?;
                let list_id = self.add_list(list)?;
                match negated {
                    false => self.rec_expr.add(TokomakExpr::InList([val_expr, list_id])),
                    true => self
                        .rec_expr
                        .add(TokomakExpr::NotInList([val_expr, list_id])),
                }
            }
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => {
                let agg_expr = TokomakExpr::AggregateBuiltin(fun);
                let fun_id = self.rec_expr.add(agg_expr);
                let args_id = self.add_list(args)?;
                match distinct {
                    true => {
                        self.rec_expr
                            .add(TokomakExpr::AggregateBuiltinDistinctCall([
                                fun_id, args_id,
                            ]))
                    }
                    false => self
                        .rec_expr
                        .add(TokomakExpr::AggregateBuiltinCall([fun_id, args_id])),
                }
            }
            //Expr::Case { expr, when_then_expr, else_expr } => todo!(),
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let sort_spec = match (asc, nulls_first) {
                    (true, true) => SortSpec::AscNullsFirst,
                    (true, false) => SortSpec::Asc,
                    (false, true) => SortSpec::Desc,
                    (false, false) => SortSpec::DescNullsFirst,
                };
                let expr_id = self.to_tokomak_expr(*expr)?;
                let spec_id = self.rec_expr.add(TokomakExpr::SortSpec(sort_spec));
                self.rec_expr.add(TokomakExpr::Sort([expr_id, spec_id]))
            }
            Expr::ScalarUDF { fun, args } => {
                let args_id = self.add_list(args)?;
                self.udf_registry.add_sudf(fun.clone());
                let fun_name: Symbol = fun.name.clone().into();
                let fun_name_id = self
                    .rec_expr
                    .add(TokomakExpr::ScalarUDF(ScalarUDFName(fun_name)));
                self.rec_expr
                    .add(TokomakExpr::ScalarUDFCall([fun_name_id, args_id]))
            }
            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            } => {
                let args_id = self.add_list(args)?;
                let partition_id = self.add_list(partition_by)?;
                let order_by_id = self.add_list(order_by)?;
                let fun_id = self.rec_expr.add(TokomakExpr::WindowBuiltin(fun));
                match window_frame {
                    Some(frame) => {
                        let frame_id = self.rec_expr.add(TokomakExpr::WindowFrame(frame));
                        self.rec_expr.add(TokomakExpr::WindowBuiltinCallFramed([
                            fun_id,
                            partition_id,
                            order_by_id,
                            frame_id,
                            args_id,
                        ]))
                    }
                    None => self.rec_expr.add(TokomakExpr::WindowBuiltinCallUnframed([
                        fun_id,
                        partition_id,
                        order_by_id,
                        args_id,
                    ])),
                }
            }
            Expr::AggregateUDF { fun, args } => {
                let args_id = self.add_list(args)?;
                self.udf_registry.add_uadf(fun.clone());
                let fun_name: Symbol = fun.name.clone().into(); //Symbols are leaked at this point in time. Maybe different solution is required.
                let fun_name_id = self
                    .rec_expr
                    .add(TokomakExpr::AggregateUDF(UDAFName(fun_name)));
                self.rec_expr
                    .add(TokomakExpr::AggregateUDFCall([fun_name_id, args_id]))
            }
            //Expr::Wildcard => todo!(),
            //Expr::ScalarVariable(_) => todo!(),

            // not yet supported
            e => {
                return Err(DataFusionError::Internal(format!(
                    "Expression not yet supported in tokomak optimizer {:?}",
                    e
                )))
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SortSpec {
    Asc,
    Desc,
    AscNullsFirst,
    DescNullsFirst,
}

impl FromStr for SortSpec {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "asc" => SortSpec::Asc,
            "desc" => SortSpec::Desc,
            "asc_nulls" => SortSpec::AscNullsFirst,
            "desc_nulls" => SortSpec::DescNullsFirst,
            _ => return Err(DataFusionError::Internal(String::new())),
        })
    }
}
impl Display for SortSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SortSpec::Asc => "asc",
                SortSpec::Desc => "desc",
                SortSpec::AscNullsFirst => "asc_nulls",
                SortSpec::DescNullsFirst => "desc_nulls",
            }
        )
    }
}

pub type Identifier = Symbol;
#[derive(Debug)]
pub enum UDF {
    Scalar(Arc<ScalarUDF>),
    Aggregate(Arc<AggregateUDF>),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct UDFName(pub Symbol);

impl Display for UDFName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl FromStr for UDFName {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let first_char = s.chars().nth(0).ok_or(DataFusionError::Internal(
            "Zero length udf name".to_string(),
        ))?;
        //for (pos, c) in s.chars().enumerate(){
        //    println!("{} - {}", pos, c);
        //}
        //println!("The first char was of the string '{}' was '{}'",s, first_char);
        if first_char == '?'
            || first_char.is_numeric()
            || first_char == '"'
            || first_char == '\''
        {
            //println!("Could not parse {} as udf name ", s);
            return Err(DataFusionError::Internal(
                "Found ? or number as first char".to_string(),
            ));
        }
        Ok(UDFName(Symbol::from_str(s).unwrap()))
    }
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]

pub struct ScalarUDFName(pub Symbol);
impl FromStr for ScalarUDFName {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 7 {
            return Err(());
        }
        match &s[0..4] {
            "udf[" => {
                if &s[s.len() - 1..s.len()] == "]" {
                    Ok(ScalarUDFName(
                        Symbol::from_str(&s[4..(s.len() - 1)]).unwrap(),
                    ))
                } else {
                    Err(())
                }
            }
            _ => Err(()),
        }
    }
}

impl Display for ScalarUDFName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "udf[{}]", self.0)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]

pub struct UDAFName(pub Symbol);
impl FromStr for UDAFName {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 8 {
            return Err(());
        }
        match &s[0..5] {
            "udaf[" => {
                if &s[s.len() - 1..s.len()] == "]" {
                    Ok(UDAFName(Symbol::from_str(&s[5..(s.len() - 1)]).unwrap()))
                } else {
                    Err(())
                }
            }
            _ => Err(()),
        }
    }
}

impl Display for UDAFName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "udaf[{}]", self.0)
    }
}

impl OptimizerRule for Tokomak {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        props: &ExecutionProps,
    ) -> DFResult<LogicalPlan> {
        let inputs = plan.inputs();
        let new_inputs: Vec<LogicalPlan> = inputs
            .iter()
            .map(|plan| self.optimize(plan, props))
            .collect::<DFResult<Vec<_>>>()?;
        // optimize all expressions individual (for now)
        let mut exprs = vec![];
        for expr in plan.expressions().iter() {
            let mut udf_registry = HashMap::new();
            let rec_expr = &mut RecExpr::default();
            let tok_expr = convert_to_tokomak_expr(expr.clone(), &mut udf_registry)
                .map_err(|e| {
                    debug!("Could not convert expression to tokomak expression: {}", e)
                })
                .ok();
            let rc_udf_reg = Rc::new(udf_registry);

            match tok_expr {
                None => exprs.push(expr.clone()),
                Some(_expr) => {
                    let udf_aware = self
                        .udf_aware_rules
                        .iter()
                        .flat_map(|f| (*f).generate_rules(Rc::clone(&rc_udf_reg)))
                        .collect::<Vec<Rewrite<TokomakExpr, ()>>>();
                    let runner = Runner::<TokomakExpr, (), ()>::default()
                        .with_expr(rec_expr)
                        .run(self.rules.iter().chain(&udf_aware));

                    let mut extractor = Extractor::new(&runner.egraph, AstSize);
                    let (_, best_expr) = extractor.find_best(runner.roots[0]);
                    match to_expr(&best_expr, &rc_udf_reg) {
                        Ok(e) => exprs.push(e),
                        Err(_) => exprs.push(expr.clone()),
                    }
                }
            }
        }

        utils::from_plan(plan, &exprs, &new_inputs)
    }

    fn name(&self) -> &str {
        "tokomak"
    }
}
