#![allow(missing_docs)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use datafusion::arrow::datatypes::DataType;
use scalar::TokomakScalar;
use std::convert::TryInto;
use std::fmt::Display;
use std::hash::Hash;

use std::str::FromStr;
use std::time::Duration;

use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::{logical_plan::LogicalPlan, optimizer::optimizer::OptimizerRule};
use log::debug;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::Expr;
use datafusion::{logical_plan::Operator, optimizer::utils};
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

#[derive(Default)]
pub struct TokomakAnalysis<T: CustomTokomakAnalysis >{
    custom: T
}

impl<T:CustomTokomakAnalysis> TokomakAnalysis<T>{
    pub fn new(analysis: T)->Self{
        Self{
            custom: analysis
        }
    }

    fn merge_builtin(&self, _to: &mut TokomakAnalysisData, _from: TokomakAnalysisData)->egg::DidMerge{
        DidMerge(false,false)
    }
}

use std::fmt::Debug;

#[derive(Debug)]
pub struct TokomakAnalysisData{
    const_folding: Option<TokomakScalar>,
}


impl TokomakAnalysisData{
    fn make<A: CustomTokomakAnalysis>(_egraph: &EGraph<TokomakExpr, TokomakAnalysis<A>>, _enode: &TokomakExpr)->Self{
        Self{
            const_folding: None,
        }
    }
}




#[derive(Debug)]
pub struct CustomTokomakData<T: Debug>{
    tokomak: TokomakAnalysisData,
    custom: T,
}


impl<T: Debug> CustomTokomakData<T>{
    fn make<A: CustomTokomakAnalysis<Data=T>>(egraph:&EGraph<TokomakExpr, TokomakAnalysis<A>>, enode: &TokomakExpr)->Self{
        Self{
            tokomak: TokomakAnalysisData::make(egraph, enode),
            custom: A::make(egraph, enode),
        }
    }
}


impl<T:CustomTokomakAnalysis> Analysis<TokomakExpr> for TokomakAnalysis<T>{
    type Data = CustomTokomakData<T::Data>;

    fn make(egraph: &EGraph<TokomakExpr, Self>, enode: &TokomakExpr) -> Self::Data {
        CustomTokomakData::make(egraph, enode) 
    }

    fn merge(&self, a: &mut Self::Data, b: Self::Data) -> egg::DidMerge {
        let merge_custom = self.custom.merge(&mut a.custom, b.custom);
        let merge_builtin = self.merge_builtin(&mut a.tokomak, b.tokomak);
        merge_custom | merge_builtin
    }

    fn modify(egraph: &mut EGraph<TokomakExpr,TokomakAnalysis<T>>, id: Id){
        T::modify(egraph, id)
    }
}

pub trait CustomTokomakAnalysis: Sized + Default{
    type Data: std::fmt::Debug;
    fn name()->&'static str;
    fn make(egraph: &EGraph<TokomakExpr, TokomakAnalysis<Self>>, enode: &TokomakExpr)->Self::Data;
    fn merge(&self, a: &mut Self::Data, b: Self::Data)->egg::DidMerge;
    #[allow(unused_variables)]
    fn modify(egraph: &mut EGraph<TokomakExpr,TokomakAnalysis<Self>>, id: Id){}
}

#[allow(unused_variables)]
impl CustomTokomakAnalysis for (){
    type Data=();
    
    fn make(egraph: &EGraph<TokomakExpr, TokomakAnalysis<Self>>, enode: &TokomakExpr)->Self::Data {}
    fn merge(&self, a: &mut Self::Data, b: Self::Data)->egg::DidMerge {
        DidMerge(false, false)
    }

    fn name()->&'static str {
        "<Default>"
    }
}







pub struct Tokomak
{
    rules: Vec<Rewrite<TokomakExpr, ()>>,
    added_builtins: BuiltinRulesFlag,
    runner_settings: RunnerSettings,
    name: String,
}



#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BuiltinRulesFlag(u32);
impl std::ops::BitOr for BuiltinRulesFlag{
    type Output=BuiltinRulesFlag;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitAnd for BuiltinRulesFlag{
    type Output = BuiltinRulesFlag;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}

impl std::ops::BitOrAssign for BuiltinRulesFlag{
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}



pub const SIMPLIFICATION_RULES: BuiltinRulesFlag = BuiltinRulesFlag(0x1);
const NO_RULES: BuiltinRulesFlag = BuiltinRulesFlag(0);


const ALL_BUILTIN_RULES: [BuiltinRulesFlag; 1] = [SIMPLIFICATION_RULES];

impl BuiltinRulesFlag{
    
    fn is_set(&self, other: BuiltinRulesFlag)->bool{
        (*self & other) != NO_RULES
    }


}
impl Default for BuiltinRulesFlag{
    fn default() -> Self {
        NO_RULES
    }
}

pub struct RunnerSettings{
    pub iter_limit: Option<usize>,
    pub node_limit: Option<usize>,
    pub time_limit: Option<Duration>,
}
impl  RunnerSettings{
    pub fn new()->Self{
        Self::default()
    }
    fn create_runner(&self)->Runner<TokomakExpr,()>{
        let mut runner = Runner::<TokomakExpr,()>::new(());
        if let Some(iter_limit) = self.iter_limit{
            runner = runner.with_iter_limit(iter_limit);
        }
        if let Some(node_limit) = self.node_limit{
            runner = runner.with_node_limit(node_limit);
        }        
        if let Some(time_limit) = self.time_limit{
            runner = runner.with_time_limit(time_limit);
        }
        runner
        //.with_scheduler(SimpleScheduler)
    }


    pub fn with_iter_limit(&mut self, iter_limit: usize)->&mut Self{
        self.iter_limit = Some(iter_limit);
        self
    }

    pub fn with_node_limit(&mut self, node_limit: usize)->&mut Self{
        self.node_limit = Some(node_limit);
        self
    }

    pub fn with_time_limit(&mut self, time_limit: Duration)->&mut Self{
        self.time_limit = Some(time_limit);
        self
    }

    pub fn optimize_exprs<C: CostFunction<TokomakExpr>>(&self, exprs: &[Expr], udf_reg: &mut HashMap<String, UDF>, rules: &[Rewrite<TokomakExpr, ()>], cost_function: C)->Result<Vec<Expr>, DataFusionError>{
        //println!("Optimizing:");
        //for e in exprs{
        //    println!("\t{:#}", e)
        //}
        let rec_exprs = exprs.iter().map(|e| convert_to_tokomak_expr(e, udf_reg)).collect::<Result<Vec<_>,_>>()?;
        let mut runner = self.create_runner();
        for expr in &rec_exprs{
            //println!("Adding {} to optimizer", expr.pretty(120));
            runner = runner.with_expr(expr);
        }
        //println!("There are {} rules", rules.len());
        runner = runner.run(rules);
        //for (idx,it) in runner.iterations.iter().enumerate(){
        //    println!("[{}]{:?}", idx, it);
        //}
        //println!("Stopped optimizing: {:#?}", runner.stop_reason);
        let mut output_expressions= Vec::with_capacity(exprs.len());
        let extractor= Extractor::new(&runner.egraph, cost_function);
        for id in &runner.roots{
            let (_, best) = extractor.find_best(*id);
            //println!("THe optimzed expr is: {}", best.pretty(120));
            let expr = to_expr(&best, udf_reg)?;
            output_expressions.push(expr);
        }
        Ok(output_expressions)
        
    }
}

impl Default for RunnerSettings {
    fn default() -> Self {
        Self::new()
    }
}


impl OptimizerRule for Tokomak{
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> DFResult<LogicalPlan> {
        let plan = utils::optimize_children(self, plan, execution_props)?;
        let inputs = plan.inputs();
        let expressions = plan.expressions();
        let mut udf_registry: HashMap<String, UDF> = HashMap::new(); 


        let optimzed_expressions = self.runner_settings.optimize_exprs(&expressions, &mut  udf_registry, &self.rules, AstSize)?;
        let inputs: Vec<LogicalPlan> = inputs.iter().map(|p| (*p).to_owned()).collect();
        utils::from_plan(&plan, &optimzed_expressions, inputs.as_slice())
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }
}




impl Tokomak{
    ///Creates a TokomakOptimizer with a custom analysis
    pub fn new( runner_settings: RunnerSettings)->Self{
        let name ="Tokomak".to_owned();
        Tokomak{
            added_builtins: NO_RULES,
            rules: Vec::new(),
            runner_settings,
            name,

        }
    }
    ///Creates a Tokomak optimizer with a custom analysis and the builtin rules defined by builtin_rules added.
    pub fn with_builtin_rules(runner_settings: RunnerSettings,  builtin_rules: BuiltinRulesFlag )->Self{
        let mut optimizer = Self::new( runner_settings);
        optimizer.add_builtin_rules(builtin_rules);
        optimizer
    }
    ///Adds the builtin rules defined by the builtin rules flag to the optimizer
    pub fn add_builtin_rules(&mut self, builtin_rules: BuiltinRulesFlag){
        for rule in ALL_BUILTIN_RULES{
            println!("The rule {:#0x?} is being added: {}",rule.0 ,builtin_rules.is_set(rule));
            //If the current flag is set and the ruleset has not been added to optimizer already
            if builtin_rules.is_set(rule) && !self.added_builtins.is_set(rule){
                match rule{
                    SIMPLIFICATION_RULES => self.add_simplification_rules(),
                    _ => panic!("Found invalid rule flag")
                }
            } 
        }
    }
    
}


fn convert_to_tokomak_expr(
    expr: &Expr,
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
            TokomakExpr::IsDistinctFrom(ids)=>self.to_binary_op(ids, Operator::IsNotDistinctFrom)?,
            TokomakExpr::IsNotDistinctFrom(ids)=>self.to_binary_op(ids, Operator::IsNotDistinctFrom)?,
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
            TokomakExpr::Column(col) => Expr::Column(col.clone()),
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
                    TokomakExpr::WindowFrame(f)=>*f,
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
                    TokomakExpr::SortSpec(s)=>*s,
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

    fn add_list(&mut self, exprs: &[Expr]) -> Result<Id, DataFusionError> {
        let list = exprs
            .iter()
            .map(|expr| self.to_tokomak_expr(expr))
            .collect::<Result<Vec<Id>, _>>()?;
        Ok(self.rec_expr.add(TokomakExpr::List(list)))
    }
    #[allow(clippy::wrong_self_convention)]
    fn to_tokomak_expr(&mut self, expr: &Expr) -> Result<Id, DataFusionError> {
        Ok(match expr {
            Expr::BinaryExpr { left, op, right } => {
                let left = self.to_tokomak_expr(left)?;
                let right = self.to_tokomak_expr(right)?;
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
                    Operator::IsDistinctFrom => TokomakExpr::IsDistinctFrom,
                    Operator::IsNotDistinctFrom => TokomakExpr::IsNotDistinctFrom,
                };
                self.rec_expr.add(binary_expr([left, right]))
            }
            Expr::Column(c) => self
                .rec_expr
                .add(TokomakExpr::Column(c.clone())),
            Expr::Literal(s) => self.rec_expr.add(TokomakExpr::Scalar(s.clone().into())),
            Expr::Not(expr) => {
                let e = self.to_tokomak_expr(expr)?;
                self.rec_expr.add(TokomakExpr::Not(e))
            }
            Expr::IsNull(expr) => {
                let e = self.to_tokomak_expr(expr)?;
                self.rec_expr.add(TokomakExpr::IsNull(e))
            }
            Expr::IsNotNull(expr) => {
                let e = self.to_tokomak_expr(expr)?;
                self.rec_expr.add(TokomakExpr::IsNotNull(e))
            }
            Expr::Negative(expr) => {
                let e = self.to_tokomak_expr(expr)?;
                self.rec_expr.add(TokomakExpr::Negative(e))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let e = self.to_tokomak_expr(expr)?;
                let low = self.to_tokomak_expr(low)?;
                let high = self.to_tokomak_expr(high)?;
                if *negated {
                    self.rec_expr
                        .add(TokomakExpr::BetweenInverted([e, low, high]))
                } else {
                    self.rec_expr.add(TokomakExpr::Between([e, low, high]))
                }
            }

            Expr::Cast { expr, data_type } => {
                let ty = data_type.clone().try_into()?;
                let e = self.to_tokomak_expr(expr)?;
                let t = self.rec_expr.add(TokomakExpr::Type(ty));

                self.rec_expr.add(TokomakExpr::Cast([e, t]))
            }
            Expr::TryCast { expr, data_type } => {
                let ty: TokomakDataType = data_type.clone().try_into()?;
                let e = self.to_tokomak_expr(expr)?;
                let t = self.rec_expr.add(TokomakExpr::Type(ty));
                self.rec_expr.add(TokomakExpr::TryCast([e, t]))
            }
            Expr::ScalarFunction { fun, args } => {
                let fun_id = self.rec_expr.add(TokomakExpr::ScalarBuiltin(fun.clone()));
                let args_id = self.add_list(args)?;
                self.rec_expr
                    .add(TokomakExpr::ScalarBuiltinCall([fun_id, args_id]))
            }
            Expr::Alias(expr, _) => self.to_tokomak_expr(expr)?,
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val_expr = self.to_tokomak_expr(expr)?;
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
                let agg_expr = TokomakExpr::AggregateBuiltin(fun.clone());
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
                let expr_id = self.to_tokomak_expr(expr)?;
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
                let fun_id = self.rec_expr.add(TokomakExpr::WindowBuiltin(fun.clone()));
                match window_frame {
                    Some(frame) => {
                        let frame_id = self.rec_expr.add(TokomakExpr::WindowFrame(frame.clone()));
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
        let first_char = s.chars().next().ok_or_else(|| DataFusionError::Internal(
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
