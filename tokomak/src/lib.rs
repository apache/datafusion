#![allow(missing_docs)]
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::PhysicalExpr;
use plan::{TokomakLogicalPlan, convert_to_df_plan};
use scalar::TokomakScalar;
use std::convert::TryInto;
use std::fmt::Display;
use std::hash::Hash;
use log::{info, error, warn};
use std::marker::PhantomData;
use std::str::FromStr;
use std::time::Duration;

use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::{logical_plan::LogicalPlan, optimizer::optimizer::OptimizerRule};
use log::debug;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::{DFSchema, Expr};
use datafusion::{logical_plan::Operator, optimizer::utils};
use std::collections::HashMap;
use std::sync::Arc;

use egg::*;

pub mod datatype;
pub mod expr;
pub mod plan;
pub mod rules;
pub mod scalar;
mod machine;
pub mod pattern;
use datatype::TokomakDataType;
use expr::{BetweenExpr, CastExpr,  FunctionCall, InListExpr, TokomakExpr, WindowBuiltinCallFramed, WindowBuiltinCallUnframed};

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

pub struct TokomakAnalysis{
    plan_schema_map: HashMap<Id, Arc<DFSchema>, fxhash::FxBuildHasher>,
    expr_alias_map: HashMap<Id, String, fxhash::FxBuildHasher>,
    expr_data_map: HashMap<Id, (DataType, bool)>,
}
impl Default for TokomakAnalysis{
    fn default() -> Self {
        Self { plan_schema_map: Default::default(), expr_alias_map: Default::default(), expr_data_map: Default::default() }
    }
}
impl TokomakAnalysis{
    pub fn add_datatype(&mut self, expr:&Expr, expr_id: Id, dt: DataType, nullable: bool)->DFResult<()>{
        match self.expr_data_map.get(&expr_id){
            Some((ty, null)) => {
                if *ty != dt || *null != nullable{
                    return Err(DataFusionError::Internal(format!("Found conflicting datatypes for the expresion {:?}. Initial {{datatype: {}, nullable {}}} new {{datatype: {}, nullable{}}}"
                    ,expr, ty, null, dt, nullable
                )))
                }
            },
            None => {
                self.expr_data_map.insert(expr_id, (dt, nullable));
            },
        }
        Ok(())
    }

    pub fn add_schema(&mut self, plan: &LogicalPlan, plan_id:Id)->DFResult<()>{
        match self.plan_schema_map.get(&plan_id){
            Some(schema) => if schema.as_ref() != plan.schema().as_ref(){
                return Err(DataFusionError::Internal(format!("Found inconsistent schemas for the plan {:?}. Initial {:?} new {:?}", plan, schema, plan.schema())));
            }else{
                ()
            },
            None => {
                self.plan_schema_map.insert(plan_id, plan.schema().clone());
            },
        }
        Ok(())
    }

    pub fn add_alias(&mut self, expr: &Expr, expr_id: Id, alias: &str)->DFResult<()>{
        match self.expr_alias_map.get(&expr_id){
            Some(existing_alias) => if alias != existing_alias{
                return Err(DataFusionError::Internal(format!("Found inconsistent aliases for the expression {:?}. Initial {:?} new {:?}", expr,  existing_alias,alias)));
            }else{
                ()
            },
            None => {
                self.expr_alias_map.insert(expr_id, alias.to_string());
            },
        }
        Ok(())
    }

}
impl Analysis<TokomakLogicalPlan> for TokomakAnalysis{
    type Data=();
    fn make(egraph: &EGraph<TokomakLogicalPlan, Self>, enode: &TokomakLogicalPlan) -> Self::Data {}
    fn merge(&mut self, a: &mut Self::Data, b: Self::Data) -> DidMerge {
        DidMerge(false, false)
    }
}

/* 
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
*/
use std::fmt::Debug;

use crate::expr::SortExpr;
/* 
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

*/


pub struct Tokomak
{
    rules: Vec<Rewrite<TokomakLogicalPlan, TokomakAnalysis>>,
    added_builtins: BuiltinRulesFlag,
    runner_settings: RunnerSettings,
    name: String,
}



#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BuiltinRulesFlag(pub(crate) u32);
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


const ALL_BUILTIN_RULES: [BuiltinRulesFlag; 2] = [EXPR_SIMPLIFICATION_RULES, PLAN_SIMPLIFICATION_RULES];

pub const EXPR_SIMPLIFICATION_RULES: BuiltinRulesFlag = BuiltinRulesFlag(0x1);
pub const PLAN_SIMPLIFICATION_RULES: BuiltinRulesFlag = BuiltinRulesFlag(0x2);
const NO_RULES: BuiltinRulesFlag = BuiltinRulesFlag(0);

pub const ALL_RULES: BuiltinRulesFlag = {
    let mut idx = 0;
    let mut flag = 0;
    while idx < ALL_BUILTIN_RULES.len(){
        flag = flag| ALL_BUILTIN_RULES[idx].0;
        idx +=1;
    }
    BuiltinRulesFlag(flag)
};


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

pub struct TokomakScheduler{
    period_length: usize,
    //default_match_limit: usize,
    //default_ban_length: usize,
    //plan_rules: indexmap::IndexMap<Symbol, RuleStats, fxhash::FxBuildHasher>,
    //expr_rules: indexmap::IndexMap<Symbol, RuleStats, fxhash::FxBuildHasher>,
}
impl TokomakScheduler{
    fn new()->Self{
        TokomakScheduler{
            period_length: 3,
        }
    }
}

#[derive(Debug)]
struct RuleStats {
    times_applied: usize,
    banned_until: usize,
    times_banned: usize,
    match_limit: usize,
    ban_length: usize,
}

impl<A: Analysis<TokomakLogicalPlan>> RewriteScheduler<TokomakLogicalPlan,A> for TokomakScheduler{
    fn can_stop(&mut self, iteration: usize) -> bool {
        true
    }

    fn search_rewrite<'a>(
        &mut self,
        iteration: usize,
        egraph: &EGraph<TokomakLogicalPlan, A>,
        rewrite: &'a Rewrite<TokomakLogicalPlan, A>,
    ) -> Vec<SearchMatches<'a, TokomakLogicalPlan>> {
        let is_plan_iter = iteration % self.period_length == 0;
        self.period_length+=1;
        if is_plan_iter {
            if !rewrite.name.as_str().starts_with("plan"){
                return vec![];
            }
        }else{
            if !rewrite.name.as_str().starts_with("expr"){
                return vec![];
            }
        }
        rewrite.search(egraph)
    }

    fn apply_rewrite(
        &mut self,
        iteration: usize,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        rewrite: &Rewrite<TokomakLogicalPlan, A>,
        matches: Vec<SearchMatches<TokomakLogicalPlan>>,
    ) -> usize {
        rewrite.apply(egraph, &matches).len()
    }
}


pub struct RunnerSettings{
    pub iter_limit: Option<usize>,
    pub node_limit: Option<usize>,
    pub time_limit: Option<Duration>,
}
impl RunnerSettings{
    pub fn new()->Self{
        Self::default()
    }
    fn create_runner(&self, egraph: EGraph<TokomakLogicalPlan, TokomakAnalysis>)->Runner<TokomakLogicalPlan,TokomakAnalysis>{
        let mut runner = Runner::default().with_egraph(egraph);
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

    pub fn optimize_plan<C: CostFunction<TokomakLogicalPlan>>(&self,root: Id, egraph: EGraph<TokomakLogicalPlan, TokomakAnalysis>, rules: &[Rewrite<TokomakLogicalPlan, TokomakAnalysis>],udf_reg: &HashMap<String, UDF>, cost_func: C)->Result<LogicalPlan, DataFusionError>{
        let mut runner = self.create_runner(egraph).with_scheduler(BackoffScheduler::default()
        .rule_match_limit("expr-rotate-and", 5000)
        .rule_match_limit("expr-rotate-or", 5000)
        .with_initial_match_limit(200)
        .with_ban_length(3) );
        runner = runner.run(rules.iter());
        for (idx,iter) in runner.iterations.iter().enumerate(){    
            info!("The iteration {} had {:?}", idx, iter);
        }

        let ex = Extractor::new(&runner.egraph, cost_func);
        let (_,plan, eclasses) = ex.find_best_with_ids(root);
        let plan = convert_to_df_plan(&plan, udf_reg, &runner.egraph.analysis, &eclasses).unwrap();
        Ok(plan)

    }

    
}

impl Default for RunnerSettings {
    fn default() -> Self {
        RunnerSettings{
            iter_limit: None,
            node_limit: None,
            time_limit: None,
        }
    }
}


impl OptimizerRule for Tokomak{
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> DFResult<LogicalPlan> {
        let udf_registry: HashMap<String, UDF> = HashMap::new(); 
        let analysis = TokomakAnalysis::default();
        let mut egraph = EGraph::new(analysis);
        let root = plan::to_tokomak_plan(plan, &mut egraph).unwrap();
        let optimized_plan = self.runner_settings.optimize_plan( root, egraph, &self.rules, &udf_registry,DefaultCostFunc )?;
        Ok(optimized_plan)   
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
            //If the current flag is set and the ruleset has not been added to optimizer already
            if builtin_rules.is_set(rule) && !self.added_builtins.is_set(rule){
                match rule{
                    EXPR_SIMPLIFICATION_RULES => self.add_expr_simplification_rules(),
                    PLAN_SIMPLIFICATION_RULES => self.add_plan_simplification_rules(),
                    _ => panic!("Found invalid rule flag")
                }
            } 
        }
    }
    
}

struct DefaultCostFunc;
const PLAN_NODE_COST: u64 = 1000;
impl CostFunction<TokomakLogicalPlan> for DefaultCostFunc{
    type Cost=u64;

    fn cost<C>(&mut self, enode: &TokomakLogicalPlan, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost {
        let op_cost = match enode{
            TokomakLogicalPlan::Filter(_) |
            TokomakLogicalPlan::Projection(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::InnerJoin(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::LeftJoin(_) => todo!(),
            TokomakLogicalPlan::RightJoin(_) => todo!(),
            TokomakLogicalPlan::FullJoin(_) => todo!(),
            TokomakLogicalPlan::SemiJoin(_) => todo!(),
            TokomakLogicalPlan::AntiJoin(_) => todo!(),
            TokomakLogicalPlan::Extension(_) => todo!(),
            TokomakLogicalPlan::CrossJoin(_) => PLAN_NODE_COST*PLAN_NODE_COST,
            TokomakLogicalPlan::Limit(_) => todo!(),
            TokomakLogicalPlan::EmptyRelation(_) => todo!(),
            TokomakLogicalPlan::Repartition(_) => todo!(),
            TokomakLogicalPlan::TableScan(_) => 0,
            TokomakLogicalPlan::Union(_) => todo!(),
            TokomakLogicalPlan::Window(_) => todo!(),
            TokomakLogicalPlan::Aggregate(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::Sort(_) => PLAN_NODE_COST ,
            TokomakLogicalPlan::PList(_) => todo!(),
            TokomakLogicalPlan::PartitioningScheme(_) => todo!(),
            //These are supporting 
            TokomakLogicalPlan::Table(_) |
            TokomakLogicalPlan::Schema(_)|
            TokomakLogicalPlan::Values(_) |
            TokomakLogicalPlan::TableProject(_)|
            TokomakLogicalPlan::LimitCount(_) |
            TokomakLogicalPlan::JoinKeys(_) |
            TokomakLogicalPlan::Str(_)|
            TokomakLogicalPlan::ExprAlias(_)|
            TokomakLogicalPlan::None => 0,
            //Expressions use AstDepth for now
            TokomakLogicalPlan::Plus(_) |
            TokomakLogicalPlan::Minus(_) |
            TokomakLogicalPlan::Multiply(_) |
            TokomakLogicalPlan::Divide(_) |
            TokomakLogicalPlan::Modulus(_) |
            TokomakLogicalPlan::Or(_) |
            TokomakLogicalPlan::And(_) |
            TokomakLogicalPlan::Eq(_) |
            TokomakLogicalPlan::NotEq(_) |
            TokomakLogicalPlan::Lt(_) |
            TokomakLogicalPlan::LtEq(_) |
            TokomakLogicalPlan::Gt(_) |
            TokomakLogicalPlan::GtEq(_) |
            TokomakLogicalPlan::RegexMatch(_) |
            TokomakLogicalPlan::RegexIMatch(_) |
            TokomakLogicalPlan::RegexNotMatch(_) |
            TokomakLogicalPlan::RegexNotIMatch(_) |
            TokomakLogicalPlan::IsDistinctFrom(_) |
            TokomakLogicalPlan::IsNotDistinctFrom(_) |
            TokomakLogicalPlan::Not(_) |
            TokomakLogicalPlan::IsNotNull(_) |
            TokomakLogicalPlan::IsNull(_) |
            TokomakLogicalPlan::Negative(_) |
            TokomakLogicalPlan::Between(_) |
            TokomakLogicalPlan::BetweenInverted(_) |
            TokomakLogicalPlan::Like(_) |
            TokomakLogicalPlan::NotLike(_) |
            TokomakLogicalPlan::InList(_) |
            TokomakLogicalPlan::NotInList(_) |
            TokomakLogicalPlan::EList(_) |
            TokomakLogicalPlan::ScalarBuiltin(_) |
            TokomakLogicalPlan::AggregateBuiltin(_) |
            TokomakLogicalPlan::WindowBuiltin(_) |
            TokomakLogicalPlan::Scalar(_) |
            TokomakLogicalPlan::ScalarBuiltinCall(_) |
            TokomakLogicalPlan::ScalarUDFCall(_) |
            TokomakLogicalPlan::AggregateBuiltinCall(_) |
            TokomakLogicalPlan::AggregateBuiltinDistinctCall(_) |
            TokomakLogicalPlan::AggregateUDFCall(_) |
            TokomakLogicalPlan::WindowBuiltinCallUnframed(_) |
            TokomakLogicalPlan::WindowBuiltinCallFramed(_) |
            TokomakLogicalPlan::SortExpr(_) |
            TokomakLogicalPlan::SortSpec(_) |
            TokomakLogicalPlan::ScalarUDF(_) |
            TokomakLogicalPlan::AggregateUDF(_) |
            TokomakLogicalPlan::Column(_) |
            TokomakLogicalPlan::WindowFrame(_) |
            TokomakLogicalPlan::Cast(_) |
            TokomakLogicalPlan::TryCast(_) => 1,

            
        };
        enode.fold(op_cost, |sum, id| sum + costs(id))
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
