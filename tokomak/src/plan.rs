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

//! Contains definitions for the plan and expression representation.
use std::cmp::Ordering;

use std::fmt;
use std::fmt::Write;
use std::hash::Hash;

use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;

use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::logical_plan::lit;
use datafusion::logical_plan::plan;
use datafusion::logical_plan::Values;

use datafusion::logical_plan::window_frames::WindowFrame;
use datafusion::logical_plan::Column;
use datafusion::logical_plan::DFField;
use datafusion::logical_plan::DFSchema;

use datafusion::logical_plan::Expr;
use datafusion::logical_plan::JoinType;
use datafusion::logical_plan::LogicalPlan;
use datafusion::logical_plan::LogicalPlanBuilder;
use datafusion::logical_plan::Operator;
use datafusion::logical_plan::Partitioning;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use egg::define_language;

use egg::EGraph;

use egg::LanguageChildren;
use egg::RecExpr;
use egg::Symbol;

use crate::expr::FunctionCall;

use crate::expr::SortExpr;
use crate::expr::TokomakColumn;
use crate::RefCount;
use crate::ScalarUDFName;
use crate::SortSpec;
use crate::TData;

use crate::TokomakAnalysis;
use crate::UDAFName;

use egg::Id;

use crate::expr::WindowBuiltinCallFramed;
use crate::expr::WindowBuiltinCallUnframed;
use crate::scalar::TokomakScalar;

use crate::TokomakDataType;
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::functions::BuiltinScalarFunction;
use datafusion::physical_plan::window_functions::WindowFunction;

//TODO: Add support for ScalarVariable expression
//TODO: Add support for IndexedFieldAccess
//TODO: Figure out if Column expressions require additional disambiguation. Are column qualifiers unique within query? If multiple of the same qualifiers are allowed
// then some way of figuring out which table/named plan a column is from is required to avoid datatype errors.
define_language! {
    /// Representation of datafusion's LogicalPlan and Expr. Note that some of these nodes may be split into multiple nodes.
    /// An example of this is [datafusion::logical_plan::plan::Repartition] which has two variants Hash and RoundRobin.
    #[allow(missing_docs)]
    pub enum TokomakLogicalPlan{
        //Logical plan nodes
        //input predicate schema
        "filter"=Filter([Id; 2]),
        //input, expressions, alias schema
        "project"=Projection([Id; 3]),
        //left right on  schema
        "inner_join"=InnerJoin([Id;4]),
        //left right on  schema
        "left_join"=LeftJoin([Id;4]),
        //left right on schema
        "right_join"=RightJoin([Id;4]),
        //left right on schema
        "full_join"=FullJoin([Id;4]),
        //left right on schema
        "semi_join"=SemiJoin([Id;4]),
        //left right on schema
        "anti_join"=AntiJoin([Id;4]),
        "ext"=Extension(UDLN),
        //left right schema
        "cross_join"=CrossJoin([Id;2]),
        //input count schema
        "limit"=Limit([Id; 2]),
        //produce_one_row schema
        "empty_relation"=EmptyRelation([Id;1]),
        "scan"=TableScan(TableScan),
        //plans alias schema
        "union"=Union([Id;2]),
        //input window_exprs schema
        "window"=Window([Id;2]),
        //input  aggr_exprs group_exprs schema
        "aggregate"=Aggregate([Id;3]),
        //input sort_exprs schema
        "sort"=Sort([Id;2]),
        "plist"=PList(Box<[Id]>),

        "round_robin"=RoundRobinBatch([Id;2]),
        //input expr_list
        "hash"=Hash([Id;3]),


        "+" = Plus([Id;2]),
        "-" = Minus([Id;2]),
        "*" = Multiply([Id;2]),
        "/" = Divide([Id;2]),
        "%" = Modulus([Id;2]),

        "or" = Or([Id;2]),
        "and" = And([Id;2]),
        "=" = Eq([Id;2]),
        "<>" = NotEq([Id;2]),
        "<" = Lt([Id;2]),
        "<=" = LtEq([Id;2]),
        ">" = Gt([Id;2]),
        ">=" = GtEq([Id;2]),
        "regex_match"=RegexMatch([Id;2]),
        "regex_imatch"=RegexIMatch([Id;2]),
        "regex_not_match"=RegexNotMatch([Id;2]),
        "regex_not_imatch"=RegexNotIMatch([Id;2]),
        "is_distinct"=IsDistinctFrom([Id;2]),
        "is_not_distinct"=IsNotDistinctFrom([Id;2]),

        "not" = Not([Id;1]),


        "is_not_null" = IsNotNull([Id;1]),
        "is_null" = IsNull([Id;1]),
        "negative" = Negative([Id;1]),
        //expr low high
        "between" = Between([Id;3]),
        "between_inverted" = BetweenInverted([Id;3]),

        "like" = Like([Id;2]),
        "not_like" = NotLike([Id;2]),
        "in_list" = InList([Id;2]),
        "not_in_list" = NotInList([Id;2]),
        "elist" = EList(Box<[Id]>),

        //ScalarValue types
        ScalarBuiltin(BuiltinScalarFunction),
        AggregateBuiltin(AggregateFunction),
        WindowBuiltin(WindowFunction),
        Scalar(TokomakScalar),

        //THe fist expression for all of the function call types must be the corresponding function type
        //For UDFs this is a string, which is looked up in the ExecutionProps
        //The last expression must be a List and is the arguments for the function.
        "call" = ScalarBuiltinCall(FunctionCall),
        "call_udf"=ScalarUDFCall(FunctionCall),
        "call_agg" = AggregateBuiltinCall(FunctionCall),
        "call_agg_distinct"=AggregateBuiltinDistinctCall(FunctionCall),
        "call_udaf" = AggregateUDFCall(FunctionCall),
        //For window fuctions index 1 is the window partition
        //index 2 is the window order
        "call_win" = WindowBuiltinCallUnframed(WindowBuiltinCallUnframed),
        //For a framed window function index 3 is the frame parameters
        "call_win_framed" = WindowBuiltinCallFramed(WindowBuiltinCallFramed),
        //Last Id of the Sort node MUST be a SortSpec
        "sort_expr" = SortExpr(SortExpr),
        //input alias
        "as"=ExprAlias([Id;2]),
        "case_lit"=CaseLit(CaseLit),
        "case_if"=CaseIf(CaseIf),
        SortSpec(SortSpec),
        ScalarUDF(ScalarUDFName),
        AggregateUDF(UDAFName),

        Column(TokomakColumn),
        WindowFrame(WindowFrame),
        // cast id as expr. Type is encoded as symbol
        "cast" = Cast([Id;2]),
        "try_cast" = TryCast([Id;2]),


        Table(Table),
        Values(SharedValues),
        TableProject(TableProject),
        Type(TokomakDataType),
        "keys"=JoinKeys([Id;2]),

        Str(Symbol),
        "None"=None,
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
///Will eventually represent userdeined logical nodes.
/// Currently does nothing.
pub struct UDLN([Id; 3]);
impl UDLN {
    #[allow(dead_code)]
    fn new(name: Id, inputs: Id, exprs: Id) -> Self {
        UDLN([name, inputs, exprs])
    }
}

impl LanguageChildren for UDLN {
    fn len(&self) -> usize {
        3
    }

    fn can_be_length(n: usize) -> bool {
        n == 3
    }

    fn from_vec(v: Vec<Id>) -> Self {
        Self(v.try_into().unwrap())
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}

/// The CASE expression is similar to a series of nested if/else and there are two forms that
/// can be used. The first form consists of a series of boolean "when" expressions with
/// corresponding "then" expressions, and an optional "else" expression.
///
/// CASE WHEN condition THEN result
///      [WHEN ...]
///      [ELSE result]
/// END
#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct CaseIf(Box<[Id]>);
impl LanguageChildren for CaseIf {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn can_be_length(n: usize) -> bool {
        n >= 2
    }

    fn from_vec(v: Vec<Id>) -> Self {
        Self(v.into_boxed_slice())
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}
impl CaseIf {
    /// Returns list of alternating when then expressions
    pub fn when_then(&self) -> &[Id] {
        if self.0.len() % 2 == 0 {
            &self.0[..]
        } else {
            &self.0[0..self.0.len() - 1]
        }
    }

    ///Returns the else expression
    pub fn else_expr(&self) -> Option<Id> {
        if self.0.len() % 2 == 0 {
            None
        } else {
            Some(*self.0.last().unwrap())
        }
    }
}
///
/// The second form uses a base expression and then a series of "when" clauses that match on a
/// literal value.
///
/// CASE expression
///     WHEN value THEN result
///     [WHEN ...]
///     [ELSE result]
/// END

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct CaseLit(Box<[Id]>);

impl LanguageChildren for CaseLit {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn can_be_length(n: usize) -> bool {
        n >= 3
    }

    fn from_vec(v: Vec<Id>) -> Self {
        CaseLit(v.into_boxed_slice())
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}

impl CaseLit {
    ///Returns the expression in the case expression
    pub fn expr(&self) -> Id {
        self.0[0]
    }
    ///Returns list of alternating when then expressions
    pub fn when_then(&self) -> &[Id] {
        if self.0.len() % 2 == 0 {
            &self.0[1..self.len() - 1]
        } else {
            &self.0[1..self.len()]
        }
    }
    ///Returns the else epxression id if it exists
    pub fn else_expr(&self) -> Option<Id> {
        //expr when then [when then]... else
        if self.0.len() % 2 == 0 {
            Some(*self.0.last().unwrap())
        } else {
            None
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone)]
///Projection of a TableScan. wrapper around Arc<Vec<usize>>
pub struct TableProject(pub Arc<Vec<usize>>);
impl fmt::Display for TableProject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0.as_ref())
    }
}
impl FromStr for TableProject {
    type Err = DataFusionError;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Err(DataFusionError::Internal(String::new()))
    }
}

impl TokomakLogicalPlan {
    ///Returns true if the node is an expression
    pub fn is_expr(&self) -> bool {
        use TokomakLogicalPlan::*;
        matches!(
            &self,
            Plus(_)
                | Minus(_)
                | Multiply(_)
                | Divide(_)
                | Modulus(_)
                | Or(_)
                | And(_)
                | Eq(_)
                | NotEq(_)
                | Lt(_)
                | LtEq(_)
                | Gt(_)
                | GtEq(_)
                | RegexMatch(_)
                | RegexIMatch(_)
                | RegexNotMatch(_)
                | RegexNotIMatch(_)
                | IsDistinctFrom(_)
                | IsNotDistinctFrom(_)
                | Not(_)
                | IsNotNull(_)
                | IsNull(_)
                | Negative(_)
                | Between(_)
                | BetweenInverted(_)
                | Like(_)
                | NotLike(_)
                | InList(_)
                | NotInList(_)
                | ScalarBuiltinCall(_)
                | ScalarUDFCall(_)
                | AggregateBuiltinCall(_)
                | AggregateBuiltinDistinctCall(_)
                | AggregateUDFCall(_)
                | WindowBuiltinCallUnframed(_)
                | WindowBuiltinCallFramed(_)
                | SortExpr(_)
                | Column(_)
                | Cast(_)
                | TryCast(_)
                | Scalar(_)
        )
    }
    ///Returns true if the node is a plan
    pub fn is_plan(&self) -> bool {
        use TokomakLogicalPlan::*;
        matches!(
            &self,
            Filter(_)
                | Projection(_)
                | InnerJoin(_)
                | LeftJoin(_)
                | RightJoin(_)
                | FullJoin(_)
                | SemiJoin(_)
                | AntiJoin(_)
                | Extension(_)
                | CrossJoin(_)
                | Limit(_)
                | EmptyRelation(_)
                | TableScan(_)
                | Union(_)
                | Window(_)
                | Aggregate(_)
                | Sort(_)
        )
    }
    ///Returns true if the node is neither a expression or plan
    pub fn is_supporting(&self) -> bool {
        use TokomakLogicalPlan::*;
        matches!(
            &self,
            EList(_) | PList(_) | Str(_) | None | Table(_) | TableProject(_)
        )
    }
}

#[derive(Clone)]
///Wrapper around TableProvider to allow it to be used in an egraph
pub struct Table(pub Arc<dyn TableProvider>);

impl FromStr for Table {
    type Err = DataFusionError;
    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Err(DataFusionError::Internal(String::new()))
    }
}
impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Table").finish()
    }
}
impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0.table_type())
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        let self_ptr = Arc::as_ptr(&self.0) as *const u8 as usize;
        let other_ptr = Arc::as_ptr(&other.0) as *const u8 as usize;
        self_ptr == other_ptr
    }
}

impl Eq for Table {}
impl Ord for Table {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_ptr = Arc::as_ptr(&self.0) as *const u8 as usize;
        let other_ptr = Arc::as_ptr(&other.0) as *const u8 as usize;
        self_ptr.cmp(&other_ptr)
    }
}
impl PartialOrd for Table {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_ptr = (Arc::as_ptr(&self.0) as *const u8) as usize;
        let other_ptr = Arc::as_ptr(&other.0) as *const u8 as usize;
        Some(self_ptr.cmp(&other_ptr))
    }
}
impl Hash for Table {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let fat_ptr = Arc::as_ptr(&self.0);
        let ptr = (fat_ptr as *const u8) as usize;
        state.write_usize(ptr);
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Hash, Clone)]
///Represents a TableScan
/// [name:str, source:Table, projection:TableProject, filters:elist, limit: Scalar(u64) | None]
pub struct TableScan([Id; 5]);
impl TableScan {
    #[allow(missing_docs)]
    pub fn new(
        table_name: Id,
        source: Id,
        projection: Id,
        filters: Id,
        limit: Id,
    ) -> Self {
        Self([table_name, source, projection, filters, limit])
    }
    ///Name id
    pub fn name(&self) -> Id {
        self.0[0]
    }
    ///Source id
    pub fn source(&self) -> Id {
        self.0[1]
    }
    ///Projection id
    pub fn projection(&self) -> Id {
        self.0[2]
    }
    ///Filters id
    pub fn filters(&self) -> Id {
        self.0[3]
    }
    ///Limit id
    pub fn limit(&self) -> Id {
        self.0[4]
    }
    ///Calculates the projected schema of the TableScan
    pub fn projected_schema(
        &self,
        egraph: &EGraph<TokomakLogicalPlan, TokomakAnalysis>,
    ) -> DFSchema {
        let name = match &egraph[self.name()].nodes[0] {
            TokomakLogicalPlan::Str(s) => s.as_str(),
            _ => panic!(),
        };
        assert!(egraph[self.source()].nodes.len() == 1);
        let source = match &egraph[self.source()].nodes[0] {
            TokomakLogicalPlan::Table(t) => &t.0,
            _ => panic!(),
        };
        assert!(egraph[self.projection()].nodes.len() == 1);
        let projection = match &egraph[self.projection()].nodes[0] {
            TokomakLogicalPlan::TableProject(t) => Some(&t.0),
            TokomakLogicalPlan::None => None,
            p => panic!(
                "TableScan expected TableProject or None as  projection found {}",
                p
            ),
        };
        let mut fields: Vec<DFField> = projection
            .as_ref()
            .map(|p| {
                p.iter()
                    .map(|i| {
                        DFField::from_qualified(name, source.schema().field(*i).clone())
                    })
                    .collect()
            })
            .unwrap_or_else(|| {
                let schema = source.schema().as_ref().clone();

                schema
                    .fields()
                    .iter()
                    .map(|f| DFField::from_qualified(name, f.clone()))
                    .collect()
            });
        fields.sort_by_key(|f| f.qualified_name());

        DFSchema::new(fields).unwrap()
    }
}

impl LanguageChildren for TableScan {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn can_be_length(n: usize) -> bool {
        n == 5
    }

    fn from_vec(v: Vec<Id>) -> Self {
        //egg requires that can_be_length is called before from_vec so this should be okay
        Self(
            v.try_into()
                .expect("Incorrect length for <TableScan as LanguageChildren>::from_vec"),
        )
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}
pub(crate) struct PlanConverter<'a> {
    pub(crate) egraph: &'a mut EGraph<TokomakLogicalPlan, TokomakAnalysis>,
}

impl<'a> PlanConverter<'a> {
    fn add_udf(&mut self, key: Symbol, udf: Arc<ScalarUDF>) -> DFResult<()> {
        //TODO: Figure out how to handle identity of udf's since they can be called without entering in registry
        // no gaurentee of uniqueness on name and comparing locations of Arc's has issues: https://stackoverflow.com/questions/67109860/how-to-compare-trait-objects-within-an-arc

        //if let Some(_existing_udf) = self.egraph.analysis.sudf_registry.insert(key, udf) {
        //    return Err(DataFusionError::Plan(format!(
        //        "Found 2 udfs with the same name in the plan: '{}'",
        //        key
        //    )));
        //}
        self.egraph.analysis.sudf_registry.insert(key, udf);
        Ok(())
    }

    fn add_udaf(&mut self, key: Symbol, udaf: Arc<AggregateUDF>) -> DFResult<()> {
        //TODO: Figure out how to handle identity of udaf's same issues as udfs
        //if let Some(_existing_udaf) = self.egraph.analysis.udaf_registry.insert(key, udaf)
        //{
        //    return Err(DataFusionError::Plan(format!(
        //        "Found 2 udafs with the same name: '{}'",
        //        key
        //    )));
        //}
        self.egraph.analysis.udaf_registry.insert(key, udaf);
        Ok(())
    }

    fn as_tokomak_expr_invariant_name(
        &mut self,
        expr: &Expr,
        schema: &DFSchema,
    ) -> DFResult<Id> {
        let invariant_alias: String;
        let (e, alias) = match expr {
            Expr::Alias(e, alias) => (e.as_ref(), alias),
            Expr::Column(_) => return self.as_tokomak_expr(expr, schema),
            e => {
                invariant_alias = e.name(schema)?;
                (e, &invariant_alias)
            }
        };
        let expr = self.as_tokomak_expr(e, schema)?;
        let alias = self.as_tokomak_str(alias.as_str());

        let aliased_expr = self
            .egraph
            .add(TokomakLogicalPlan::ExprAlias([expr, alias]));
        Ok(aliased_expr)
    }

    fn set_datatype(&mut self, id: Id, dt: DataType) -> DFResult<()> {
        let dt = match &self.egraph[id].data {
            TData::DataType(existing) => {
                if existing.as_ref() != &dt {
                    return Err(DataFusionError::Plan(format!("TokomakOptimizer found mismatched expression: existing={} new={}",existing, dt)));
                } else {
                    return Ok(());
                }
            }
            TData::Schema(s) => {
                return Err(DataFusionError::Plan(format!(
                    "TokomakOptimizer found schema instead of datatype: {}",
                    s
                )))
            }
            _ => {
                let dt = if self.egraph.analysis.datatype_cache.contains(&dt) {
                    self.egraph
                        .analysis
                        .datatype_cache
                        .get(&dt)
                        .unwrap()
                        .clone()
                } else {
                    RefCount::new(dt)
                };
                dt
            }
        };
        self.egraph[id].data = TData::DataType(dt);
        Ok(())
    }

    pub(crate) fn as_tokomak_expr(
        &mut self,
        expr: &Expr,
        schema: &DFSchema,
    ) -> Result<Id, DataFusionError> {
        let datatype = expr.get_type(schema)?;

        let texpr: TokomakLogicalPlan = match expr {
            Expr::Alias(e, alias) => {
                let id = self.as_tokomak_expr(e, schema)?;
                let alias = self.as_tokomak_str(alias.as_str());
                TokomakLogicalPlan::ExprAlias([id, alias])
            }
            Expr::BinaryExpr { left, op, right } => {
                let left = self.as_tokomak_expr(left, schema)?;
                let right = self.as_tokomak_expr(right, schema)?;
                (match op {
                    Operator::Eq => TokomakLogicalPlan::Eq,
                    Operator::NotEq => TokomakLogicalPlan::NotEq,
                    Operator::Lt => TokomakLogicalPlan::Lt,
                    Operator::LtEq => TokomakLogicalPlan::LtEq,
                    Operator::Gt => TokomakLogicalPlan::Gt,
                    Operator::GtEq => TokomakLogicalPlan::GtEq,
                    Operator::Plus => TokomakLogicalPlan::Plus,
                    Operator::Minus => TokomakLogicalPlan::Minus,
                    Operator::Multiply => TokomakLogicalPlan::Multiply,
                    Operator::Divide => TokomakLogicalPlan::Divide,
                    Operator::Modulo => TokomakLogicalPlan::Modulus,
                    Operator::And => TokomakLogicalPlan::And,
                    Operator::Or => TokomakLogicalPlan::Or,
                    Operator::Like => TokomakLogicalPlan::Like,
                    Operator::NotLike => TokomakLogicalPlan::NotLike,
                    Operator::RegexMatch => TokomakLogicalPlan::RegexMatch,
                    Operator::RegexIMatch => TokomakLogicalPlan::RegexIMatch,
                    Operator::RegexNotMatch => TokomakLogicalPlan::RegexNotMatch,
                    Operator::RegexNotIMatch => TokomakLogicalPlan::RegexNotIMatch,
                    Operator::IsDistinctFrom => TokomakLogicalPlan::IsDistinctFrom,
                    Operator::IsNotDistinctFrom => TokomakLogicalPlan::IsNotDistinctFrom,
                })([left, right])
            }
            Expr::Column(c) => TokomakLogicalPlan::Column(c.clone().into()),
            Expr::Literal(s) => TokomakLogicalPlan::Scalar(s.clone().try_into()?),
            Expr::Not(expr) => {
                let e = self.as_tokomak_expr(expr, schema)?;
                TokomakLogicalPlan::Not([e])
            }
            Expr::IsNull(expr) => {
                let e = self.as_tokomak_expr(expr, schema)?;
                TokomakLogicalPlan::IsNull([e])
            }
            Expr::IsNotNull(expr) => {
                let e = self.as_tokomak_expr(expr, schema)?;
                TokomakLogicalPlan::IsNotNull([e])
            }
            Expr::Negative(expr) => {
                let e = self.as_tokomak_expr(expr, schema)?;
                TokomakLogicalPlan::Negative([e])
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let e = self.as_tokomak_expr(expr, schema)?;
                let low = self.as_tokomak_expr(low, schema)?;
                let high = self.as_tokomak_expr(high, schema)?;
                if *negated {
                    TokomakLogicalPlan::BetweenInverted([e, low, high])
                } else {
                    TokomakLogicalPlan::Between([e, low, high])
                }
            }

            Expr::Cast { expr, data_type } => {
                let e = self.as_tokomak_expr(expr, schema)?;
                let tdt: TokomakDataType = data_type.clone().try_into()?;
                let dt = self.egraph.add(TokomakLogicalPlan::Type(tdt));
                TokomakLogicalPlan::Cast([e, dt])
            }
            Expr::TryCast { expr, data_type } => {
                let e = self.as_tokomak_expr(expr, schema)?;
                let tdt: TokomakDataType = data_type.clone().try_into()?;
                let dt = self.egraph.add(TokomakLogicalPlan::Type(tdt));
                TokomakLogicalPlan::TryCast([e, dt])
            }
            Expr::ScalarFunction { fun, args } => {
                let func = self
                    .egraph
                    .add(TokomakLogicalPlan::ScalarBuiltin(fun.clone()));
                let args = self.as_tokomak_expr_list(args, schema)?;
                TokomakLogicalPlan::ScalarBuiltinCall(FunctionCall::new(func, args))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = self.as_tokomak_expr(expr, schema)?;
                let list = self.as_tokomak_expr_list(list, schema)?;
                let inlist = [expr, list];
                match negated {
                    false => TokomakLogicalPlan::InList(inlist),
                    true => TokomakLogicalPlan::NotInList(inlist),
                }
            }
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => {
                let func = self
                    .egraph
                    .add(TokomakLogicalPlan::AggregateBuiltin(fun.clone()));
                let args = self.as_tokomak_expr_list(args, schema)?;
                let fun_call = FunctionCall::new(func, args);
                match distinct {
                    true => TokomakLogicalPlan::AggregateBuiltinDistinctCall(fun_call),
                    false => TokomakLogicalPlan::AggregateBuiltinCall(fun_call),
                }
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let mut exprs = Vec::with_capacity(
                    expr.is_some() as usize
                        + when_then_expr.len() * 2
                        + else_expr.is_some() as usize,
                );
                if let Some(e) = expr {
                    let e = self.as_tokomak_expr(e, schema)?;
                    exprs.push(e);
                }
                for (when, then) in when_then_expr {
                    println!("when: {} then:{}", when, then);

                    let when = self.as_tokomak_expr(when, schema)?;
                    let then = self.as_tokomak_expr(then, schema)?;
                    exprs.push(when);
                    exprs.push(then);
                }
                if let Some(else_expr) = else_expr {
                    let els = self.as_tokomak_expr(else_expr, schema)?;
                    exprs.push(els);
                }
                if expr.is_some() {
                    TokomakLogicalPlan::CaseLit(CaseLit::from_vec(exprs))
                } else {
                    TokomakLogicalPlan::CaseIf(CaseIf::from_vec(exprs))
                }
            }
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let sort_spec = match (asc, nulls_first) {
                    (true, true) => SortSpec::AscNullsFirst,
                    (true, false) => SortSpec::Asc,
                    (false, true) => SortSpec::DescNullsFirst,
                    (false, false) => SortSpec::Desc,
                };
                let expr_id = self.as_tokomak_expr(expr, schema)?;
                let spec_id = self.egraph.add(TokomakLogicalPlan::SortSpec(sort_spec));
                TokomakLogicalPlan::SortExpr(SortExpr::new(expr_id, spec_id))
            }
            Expr::ScalarUDF { fun, args } => {
                let fun_name: Symbol = fun.name.clone().into();
                self.add_udf(fun_name, fun.clone())?;
                let func = self
                    .egraph
                    .add(TokomakLogicalPlan::ScalarUDF(ScalarUDFName(fun_name)));
                let args = self.as_tokomak_expr_list(args, schema)?;
                let func_call = FunctionCall::new(func, args);
                TokomakLogicalPlan::ScalarUDFCall(func_call)
            }
            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            } => {
                let args = self.as_tokomak_expr_list(args, schema)?;
                let partition = self.as_tokomak_expr_list(partition_by, schema)?;
                let order_by = self.as_tokomak_expr_list(order_by, schema)?;
                let fun_id = self
                    .egraph
                    .add(TokomakLogicalPlan::WindowBuiltin(fun.clone()));

                match window_frame {
                    Some(frame) => {
                        let frame_id =
                            self.egraph.add(TokomakLogicalPlan::WindowFrame(*frame));
                        TokomakLogicalPlan::WindowBuiltinCallFramed(
                            WindowBuiltinCallFramed::new(
                                fun_id, args, partition, order_by, frame_id,
                            ),
                        )
                    }
                    None => TokomakLogicalPlan::WindowBuiltinCallUnframed(
                        WindowBuiltinCallUnframed::new(fun_id, args, partition, order_by),
                    ),
                }
            }
            Expr::AggregateUDF { fun, args } => {
                let fun_name: Symbol = fun.name.clone().into(); //Symbols are leaked at this point in time. Maybe different solution is required.
                let func = self
                    .egraph
                    .add(TokomakLogicalPlan::AggregateUDF(UDAFName(fun_name)));
                let args = self.as_tokomak_expr_list(args, schema)?;
                let func_call = FunctionCall::new(func, args);
                self.add_udaf(fun_name, fun.clone())?;
                TokomakLogicalPlan::AggregateUDFCall(func_call)
            }
            e => {
                return Err(DataFusionError::Internal(format!(
                    "Expression not yet supported in tokomak optimizer {:?}",
                    e
                )))
            }
        };
        //Ensure that the datatypes for all inserted nodes is consistent

        let id = self.egraph.add(texpr);
        self.set_datatype(id, datatype)?;
        Ok(id)
    }

    fn as_tokomak_expr_list(
        &mut self,
        exprs: &[Expr],
        schema: &DFSchema,
    ) -> Result<Id, DataFusionError> {
        let ids = exprs
            .iter()
            .map(|e| self.as_tokomak_expr(e, schema))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self
            .egraph
            .add(TokomakLogicalPlan::EList(ids.into_boxed_slice())))
    }

    fn as_tokomak_projection_list(
        &mut self,
        exprs: &[Expr],
        schema: &DFSchema,
    ) -> Result<Id, DataFusionError> {
        let ids = exprs
            .iter()
            .map(|e| self.as_tokomak_expr_invariant_name(e, schema))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self
            .egraph
            .add(TokomakLogicalPlan::EList(ids.into_boxed_slice())))
    }

    fn as_tokomak_plan_list(
        &mut self,
        plans: &[LogicalPlan],
    ) -> Result<Id, DataFusionError> {
        let ids = Vec::with_capacity(plans.len());
        for plan in plans {
            let id = self.as_tokomak_plan(plan)?;
            self.set_schema(id, plan.schema()).unwrap();
        }
        Ok(self
            .egraph
            .add(TokomakLogicalPlan::PList(ids.into_boxed_slice())))
    }

    fn set_schema(
        &mut self,
        plan_id: Id,
        schema: &DFSchema,
    ) -> Result<(), DataFusionError> {
        let s: RefCount<DFSchema> = match &self.egraph[plan_id].data {
            TData::DataType(dt) => {
                return Err(DataFusionError::Plan(format!(
                "TokomakOptimizer attempted to add schema found existing datatype: {}",
                dt
            )))
            }
            _ => {
                let schema = match self.egraph.analysis.schema_cache.get(schema) {
                    Some(s) => s.clone(),
                    None => {
                        let mut fields = schema.fields().clone();
                        fields.sort_by_key(|field| field.qualified_column());
                        RefCount::new(DFSchema::new(fields)?)
                    }
                };
                schema
            }
        };
        self.egraph[plan_id].data = TData::Schema(s.clone());
        self.egraph.analysis.schema_cache.insert(s);
        Ok(())
    }

    fn as_tokomak_str<'b>(&mut self, s: impl Into<Option<&'b str>>) -> Id {
        let s = s.into();
        match s {
            Some(s) => self.egraph.add(TokomakLogicalPlan::Str(s.into())),
            None => self.egraph.add(TokomakLogicalPlan::None),
        }
    }

    fn as_join_keys(
        &mut self,
        on: &[(Column, Column)],
        lschema: &Arc<DFSchema>,
        rschema: &Arc<DFSchema>,
    ) -> Result<Id, DataFusionError> {
        let mut left_ids = Vec::with_capacity(on.len());
        let mut right_ids = Vec::with_capacity(on.len());
        for (l, r) in on {
            let lid = self.egraph.add(TokomakLogicalPlan::Column(l.into()));
            let rid = self.egraph.add(TokomakLogicalPlan::Column(r.into()));
            let lfield = lschema.field_from_column(l)?;
            let rfield = rschema.field_from_column(r)?;
            self.set_datatype(lid, lfield.data_type().clone())?;
            self.set_datatype(rid, rfield.data_type().clone())?;
            left_ids.push(lid);
            right_ids.push(rid);
        }

        let l = left_ids.into_boxed_slice();
        let l = self.egraph.add(TokomakLogicalPlan::EList(l));
        let r = right_ids.into_boxed_slice();
        let r = self.egraph.add(TokomakLogicalPlan::EList(r));

        Ok(self.egraph.add(TokomakLogicalPlan::JoinKeys([l, r])))
    }

    fn convert_projection(
        &mut self,
        projection: &plan::Projection,
    ) -> Result<TokomakLogicalPlan, DataFusionError> {
        //All conversion functions should use structured bindings so that new properties being added is a hard error as that might affect
        //the validity of some optimizations
        let plan::Projection {
            input,
            expr,
            schema: _schema,
            alias,
        } = projection;
        let exprs = self.as_tokomak_projection_list(expr, input.schema())?;
        let input = self.as_tokomak_plan(input)?;
        let alias = self.as_tokomak_str(alias.as_ref().map(|s| s.as_str()));
        Ok(TokomakLogicalPlan::Projection([input, exprs, alias]))
    }

    fn convert_filter(&mut self, filter: &plan::Filter) -> DFResult<TokomakLogicalPlan> {
        //Since all of the rules dealing with filters are of the form (filter ?input_plan (and ?predicate_of_interest ?other))
        //add a dummy value to the predicate so that teh above pattern will still match if only the predicate of interest is present.
        //The Filter removal rule will remove all filter plans with literal true value
        let plan::Filter { predicate, input } = filter;
        let predicate = predicate.clone().and(lit(true));
        let predicate = self.as_tokomak_expr(&predicate, input.schema())?;
        let input = self.as_tokomak_plan(input)?;
        Ok(TokomakLogicalPlan::Filter([input, predicate]))
    }

    fn convert_window(&mut self, window: &plan::Window) -> DFResult<TokomakLogicalPlan> {
        let plan::Window {
            window_expr,
            input,
            schema: _schema,
        } = window;
        let window_expr = self.as_tokomak_projection_list(window_expr, input.schema())?;
        let input = self.as_tokomak_plan(input)?;
        Ok(TokomakLogicalPlan::Window([input, window_expr]))
    }

    fn convert_aggregate(
        &mut self,
        aggregate: &plan::Aggregate,
    ) -> DFResult<TokomakLogicalPlan> {
        let plan::Aggregate {
            input,
            aggr_expr,
            group_expr,
            schema: _schema,
        } = aggregate;
        let group_expr = self.as_tokomak_expr_list(group_expr, input.schema())?;
        let aggr_expr = self.as_tokomak_projection_list(aggr_expr, input.schema())?;
        let input = self.as_tokomak_plan(input)?;
        Ok(TokomakLogicalPlan::Aggregate([
            input, aggr_expr, group_expr,
        ]))
    }

    fn convert_sort(&mut self, sort: &plan::Sort) -> DFResult<TokomakLogicalPlan> {
        let plan::Sort { input, expr } = sort;
        let exprs = self.as_tokomak_expr_list(expr, sort.input.schema())?;
        let input = self.as_tokomak_plan(input)?;
        Ok(TokomakLogicalPlan::Sort([input, exprs]))
    }

    fn convert_join(&mut self, join: &plan::Join) -> DFResult<TokomakLogicalPlan> {
        let plan::Join {
            left,
            right,
            on,
            join_type,
            join_constraint: _join_constraint,
            schema: _schema,
            null_equals_null,
        } = join;
        let on = self.as_join_keys(on, left.schema(), right.schema())?;
        let left = self.as_tokomak_plan(left)?;
        let right = self.as_tokomak_plan(right)?;
        let null_equals_null =
            self.egraph
                .add(TokomakLogicalPlan::Scalar(TokomakScalar::Boolean(Some(
                    *null_equals_null,
                ))));
        Ok((match join_type {
            datafusion::logical_plan::JoinType::Inner => TokomakLogicalPlan::InnerJoin,
            datafusion::logical_plan::JoinType::Left => TokomakLogicalPlan::LeftJoin,
            datafusion::logical_plan::JoinType::Right => TokomakLogicalPlan::RightJoin,
            datafusion::logical_plan::JoinType::Full => TokomakLogicalPlan::FullJoin,
            datafusion::logical_plan::JoinType::Semi => TokomakLogicalPlan::SemiJoin,
            datafusion::logical_plan::JoinType::Anti => TokomakLogicalPlan::AntiJoin,
        })([left, right, on, null_equals_null]))
    }

    fn convert_crossjoin(
        &mut self,
        cross: &plan::CrossJoin,
    ) -> DFResult<TokomakLogicalPlan> {
        let plan::CrossJoin {
            left,
            right,
            schema: _schema,
        } = cross;
        let left = self.as_tokomak_plan(left)?;
        let right = self.as_tokomak_plan(right)?;
        Ok(TokomakLogicalPlan::CrossJoin([left, right]))
    }

    fn convert_union(&mut self, union: &plan::Union) -> DFResult<TokomakLogicalPlan> {
        let plan::Union {
            inputs,
            alias,
            schema: _schema,
        } = union;
        let plans = self.as_tokomak_plan_list(inputs)?;
        let alias = self.as_tokomak_str(alias.as_ref().map(|s| s.as_str()));
        Ok(TokomakLogicalPlan::Union([plans, alias]))
    }

    fn convert_table_scan(
        &mut self,
        table_scan: &plan::TableScan,
    ) -> DFResult<TokomakLogicalPlan> {
        let plan::TableScan {
            table_name,
            source,
            projection,
            projected_schema: _projected_schema,
            limit,
            filters,
        } = table_scan;
        let schema = source.schema().as_ref().clone();
        let dfschema = DFSchema::try_from_qualified_schema(table_name.as_str(), &schema)?;

        let filters = self.as_tokomak_expr_list(filters, &dfschema)?;
        let source = self
            .egraph
            .add(TokomakLogicalPlan::Table(Table(source.clone())));
        let table_name = self.as_tokomak_str(table_name.as_str());
        let projection = match &projection {
            Some(proj) => {
                self.egraph
                    .add(TokomakLogicalPlan::TableProject(TableProject(Arc::new(
                        proj.clone(),
                    ))))
            }
            None => self.egraph.add(TokomakLogicalPlan::None),
        };
        let limit = self.egraph.add(match limit {
            Some(lim) => {
                TokomakLogicalPlan::Scalar(TokomakScalar::UInt64(Some(*lim as u64)))
            }
            None => TokomakLogicalPlan::None,
        });
        Ok(TokomakLogicalPlan::TableScan(TableScan::new(
            table_name, source, projection, filters, limit,
        )))
    }

    fn convert_empty_relation(
        &mut self,
        empty_relation: &plan::EmptyRelation,
    ) -> DFResult<TokomakLogicalPlan> {
        let plan::EmptyRelation {
            produce_one_row,
            schema: _schema,
        } = empty_relation;
        let produce_one_row = TokomakScalar::Boolean(Some(*produce_one_row));
        let produce_one_row =
            self.egraph.add(TokomakLogicalPlan::Scalar(produce_one_row));
        Ok(TokomakLogicalPlan::EmptyRelation([produce_one_row]))
    }
    fn convert_limit(&mut self, limit: &plan::Limit) -> DFResult<TokomakLogicalPlan> {
        let plan::Limit { n, input } = limit;
        let n = self
            .egraph
            .add(TokomakLogicalPlan::Scalar(TokomakScalar::UInt64(Some(
                *n as u64,
            ))));
        let input = self.as_tokomak_plan(input)?;
        Ok(TokomakLogicalPlan::Limit([input, n]))
    }

    fn convert_extension(
        &mut self,
        _ext: &plan::Extension,
    ) -> DFResult<TokomakLogicalPlan> {
        Err(DataFusionError::NotImplemented(String::from(
            "The tokomak optimizer does not support logical extension plan nodes yet",
        )))
        //let plan::Extension{node} = ext;
        //let name = node.name();
        //let inputs = node.inputs().into_iter().map(|l| l.clone()).collect::<Vec<_>>();
        //let exprs = node.expressions();
        //let plist = self.as_tokomak_plan_list(&inputs)?;
        //let elist = self.as_tokomak_expr_list(&exprs, schema)?;
        //let name_id = self.as_tokomak_str(name);
        //Ok(TokomakLogicalPlan::Extension(UDLN::new(name_id, plist, elist)))
    }

    fn as_tokomak_plan(&mut self, plan: &LogicalPlan) -> Result<Id, DataFusionError> {
        let tplan = match plan{
            LogicalPlan::Projection(p) => self.convert_projection(p)?,
            LogicalPlan::Filter (f ) => self.convert_filter(f)?,
            LogicalPlan::Window ( w ) => self.convert_window(w)?,
            LogicalPlan::Aggregate (a)=>self.convert_aggregate(a)?,
            LogicalPlan::Sort (s)=>self.convert_sort(s)?,
            LogicalPlan::Join (j)=>self.convert_join(j)?,
            LogicalPlan::CrossJoin(c)=>self.convert_crossjoin(c)?,
            LogicalPlan::Repartition(r) => {
                let input = self.as_tokomak_plan(&r.input)?;
                match &r.partitioning_scheme{
                    datafusion::logical_plan::Partitioning::RoundRobinBatch(size) => {
                        let size = TokomakLogicalPlan::Scalar(TokomakScalar::UInt64(Some(*size as u64)));
                        let size_id = self.egraph.add(size);
                        TokomakLogicalPlan::RoundRobinBatch([input, size_id])
                    },
                    datafusion::logical_plan::Partitioning::Hash(expr, size) => {
                        let s = TokomakLogicalPlan::Scalar(TokomakScalar::UInt64(Some(*size as u64)));
                        let size_id = self.egraph.add(s);
                        let expr_list = self.as_tokomak_expr_list(expr, r.input.schema())?;
                        TokomakLogicalPlan::Hash([input, expr_list, size_id])
                    },
                }
            },
            LogicalPlan::Union(u)=> self.convert_union(u)?,
            LogicalPlan::TableScan(t)=>self.convert_table_scan(t)?,
            LogicalPlan::EmptyRelation (e) => self.convert_empty_relation(e)?,
            LogicalPlan::Limit (l)=> self.convert_limit(l)?,

            LogicalPlan::Extension(e)=>self.convert_extension(e)?,
            LogicalPlan::Values(values)=>{
                let shared_vals = SharedValues(Arc::new(values.values.clone()));
                TokomakLogicalPlan::Values(shared_vals)
            }
            plan @ (LogicalPlan::CreateExternalTable (_) |
            LogicalPlan::Explain (_) |
            LogicalPlan::Analyze (_) |
            LogicalPlan::CreateMemoryTable(_)|
            LogicalPlan::DropTable(_)) =>  return Err(DataFusionError::NotImplemented(format!("The logical plan {:?} will not be implemented for the Tokomak optimizer", plan))),
        };
        let id = self.egraph.add(tplan);

        self.set_schema(id, plan.schema()).unwrap();
        Ok(id)
    }
}

pub(crate) fn to_tokomak_plan(
    plan: &LogicalPlan,
    egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>,
) -> Result<Id, DataFusionError> {
    let mut converter = PlanConverter { egraph };
    converter.as_tokomak_plan(plan)
}

fn unexpected_node(
    curr_node: &'static str,
    unexpected_plan: &TokomakLogicalPlan,
    expected: &'static str,
) -> DataFusionError {
    DataFusionError::Internal(format!(
        "{} expected a node of type {} found {:?}",
        curr_node, expected, unexpected_plan
    ))
}

fn unexpected_expr(
    node: &TokomakLogicalPlan,
    parent: &'static str,
    expected: &'static str,
) -> DataFusionError {
    DataFusionError::Internal(format!(
        "{} Expected node of {}, found {:?}",
        parent, expected, node
    ))
}

pub(crate) struct TokomakPlanConverter<'a> {
    refs: &'a [TokomakLogicalPlan],
    eclasses: &'a [Id],
    egraph: &'a EGraph<TokomakLogicalPlan, TokomakAnalysis>,
}
impl<'a> TokomakPlanConverter<'a> {
    pub(crate) fn new(
        rec_expr: &'a RecExpr<TokomakLogicalPlan>,
        eclasses: &'a [Id],
        egraph: &'a EGraph<TokomakLogicalPlan, TokomakAnalysis>,
    ) -> Self {
        Self {
            refs: rec_expr.as_ref(),
            eclasses,
            egraph,
        }
    }
    fn get_ref(&self, id: Id) -> &TokomakLogicalPlan {
        let idx: usize = id.into();
        &self.refs[idx]
    }
    fn convert_inlist(
        &self,
        [expr, list]: &[Id; 2],
        negated: bool,
    ) -> Result<Expr, DataFusionError> {
        let name = if negated { "NotInList" } else { "InList" };
        let elist = match self.get_ref(*list) {
            TokomakLogicalPlan::EList(l) => l,
            node => return Err(unexpected_expr(node, name, "List")),
        };
        let expr = self.convert_to_expr(*expr)?.into();

        let list = elist
            .iter()
            .map(|i| self.convert_to_expr(*i))
            .collect::<DFResult<Vec<_>>>()?;
        Ok(Expr::InList {
            expr,
            list,
            negated,
        })
    }

    fn to_when_then_list(
        &self,
        when_then: &[Id],
    ) -> DFResult<Vec<(Box<Expr>, Box<Expr>)>> {
        if when_then.len() % 2 != 0 || when_then.len() <= 1 {
            return Err(DataFusionError::Internal(format!(
                "When then list must be even in length and greater than 0, found: {}",
                when_then.len()
            )));
        }
        let mut wt = Vec::with_capacity(when_then.len() / 2);
        for win in when_then.chunks_exact(2) {
            let when = self.convert_to_expr(win[0])?.into();
            let then = self.convert_to_expr(win[1])?.into();
            wt.push((when, then));
        }
        Ok(wt)
    }
    fn to_binary_op(
        &self,
        [left, right]: &[Id; 2],
        op: Operator,
    ) -> Result<Expr, DataFusionError> {
        let left = self.convert_to_expr(*left)?;
        let right = self.convert_to_expr(*right)?;
        Ok(datafusion::logical_plan::binary_expr(left, op, right))
    }
    fn extract_usize_from_scalar(
        &self,
        s: &TokomakScalar,
        parent: &'static str,
    ) -> Result<usize, DataFusionError> {
        let ret_val = match s {
            TokomakScalar::Int8(Some(v)) => {
                if *v <= 0 {
                    return Err(DataFusionError::Plan(format!(
                        "TokomakOptimizer {} expected usize found negative value: {}",
                        parent, v
                    )));
                } else {
                    *v as usize
                }
            }
            TokomakScalar::Int16(Some(v)) => {
                if *v <= 0 {
                    return Err(DataFusionError::Plan(format!(
                        "TokomakOptimizer {} expected usize found negative value: {}",
                        parent, v
                    )));
                } else {
                    *v as usize
                }
            }
            TokomakScalar::Int32(Some(v)) => {
                if *v <= 0 {
                    return Err(DataFusionError::Plan(format!(
                        "TokomakOptimizer {} expected usize found negative value: {}",
                        parent, v
                    )));
                } else {
                    *v as usize
                }
            }
            TokomakScalar::Int64(Some(v)) => {
                if *v <= 0 {
                    return Err(DataFusionError::Plan(format!(
                        "TokomakOptimizer {} expected usize found negative value: {}",
                        parent, v
                    )));
                } else {
                    *v as usize
                }
            }
            TokomakScalar::UInt8(Some(v)) => *v as usize,
            TokomakScalar::UInt16(Some(v)) => *v as usize,
            TokomakScalar::UInt32(Some(v)) => *v as usize,
            TokomakScalar::UInt64(Some(v)) => *v as usize,
            v => {
                return Err(DataFusionError::Plan(format!(
                    "TokomakOptimizer {} expected usize found scalar value {}",
                    parent, v
                )))
            }
        };
        Ok(ret_val)
    }
    fn extract_usize(
        &self,
        id: Id,
        parent: &'static str,
    ) -> Result<usize, DataFusionError> {
        let scalar = match self.get_ref(id) {
            TokomakLogicalPlan::Scalar(s) => s,
            p => {
                return Err(DataFusionError::Plan(format!(
                    "TokomakOptimizer {} expected scalar usize, found {:?}",
                    parent, p
                )))
            }
        };
        self.extract_usize_from_scalar(scalar, parent)
    }
    fn extract_datatype(
        &self,
        id: Id,
        parent: &'static str,
    ) -> Result<DataType, DataFusionError> {
        let dt = match self.get_ref(id) {
            TokomakLogicalPlan::Type(ty) => ty,
            node => return Err(unexpected_expr(node, parent, "Type")),
        };
        Ok(dt.clone().into())
    }

    pub(crate) fn convert_to_expr(&self, id: Id) -> Result<Expr, DataFusionError> {
        let expr = match self.get_ref(id) {
                TokomakLogicalPlan::ExprAlias([e, alis])=>{
                    let e = self.convert_to_expr(*e)?;
                    let alias = self.extract_alias(*alis, "Expr::Alias")?;
                    let alias = match alias{
                        Some(s) => s,
                        None => return Err(DataFusionError::Plan("TokomakOptimizer found None when attempting to extract alias for Expr::Alias".to_string())),
                    };
                    Expr::Alias(e.into(), alias)
                }
                TokomakLogicalPlan::Plus(ids) => self.to_binary_op(ids, Operator::Plus)?,
                TokomakLogicalPlan::Minus(ids) => self.to_binary_op(ids, Operator::Minus)?,
                TokomakLogicalPlan::Divide(ids) =>self.to_binary_op(ids, Operator::Divide)?,
                TokomakLogicalPlan::Modulus(ids) => self.to_binary_op(ids, Operator::Modulo)?,
                TokomakLogicalPlan::Multiply(ids) => self.to_binary_op(ids, Operator::Multiply)?,
                TokomakLogicalPlan::Or(ids) => self.to_binary_op(ids, Operator::Or)?,
                TokomakLogicalPlan::And(ids) => self.to_binary_op(ids, Operator::And)?,
                TokomakLogicalPlan::Eq(ids) => self.to_binary_op(ids, Operator::Eq)?,
                TokomakLogicalPlan::NotEq(ids) => self.to_binary_op(ids, Operator::NotEq)?,
                TokomakLogicalPlan::Lt(ids) => self.to_binary_op(ids, Operator::Lt)?,
                TokomakLogicalPlan::LtEq(ids) =>self.to_binary_op(ids, Operator::LtEq)?,
                TokomakLogicalPlan::Gt(ids) => self.to_binary_op(ids, Operator::Gt)?,
                TokomakLogicalPlan::GtEq(ids) => self.to_binary_op(ids, Operator::GtEq)?,
                TokomakLogicalPlan::Like(ids) => self.to_binary_op(ids, Operator::Like)?,
                TokomakLogicalPlan::NotLike(ids) => self.to_binary_op(ids, Operator::NotLike)?,
                TokomakLogicalPlan::RegexMatch(ids) => self.to_binary_op(ids, Operator::RegexMatch)?,
                TokomakLogicalPlan::RegexIMatch(ids) => self.to_binary_op(ids, Operator::RegexIMatch)?,
                TokomakLogicalPlan::RegexNotMatch(ids) => self.to_binary_op(ids, Operator::RegexNotMatch)?,
                TokomakLogicalPlan::RegexNotIMatch(ids) => self.to_binary_op(ids, Operator::RegexNotIMatch)?,
                TokomakLogicalPlan::IsDistinctFrom(ids)=>self.to_binary_op(ids, Operator::IsNotDistinctFrom)?,
                TokomakLogicalPlan::IsNotDistinctFrom(ids)=>self.to_binary_op(ids, Operator::IsNotDistinctFrom)?,
                TokomakLogicalPlan::Not(expr) => {
                    let l = self.convert_to_expr(expr[0])?;
                    Expr::Not(Box::new(l))
                }
                TokomakLogicalPlan::IsNotNull(expr) => {
                    let l = self.convert_to_expr(expr[0])?;
                    Expr::IsNotNull(Box::new(l))
                }
                TokomakLogicalPlan::IsNull(expr) => {
                    let l = self.convert_to_expr(expr[0])?;
                    Expr::IsNull(Box::new(l))
                }
                TokomakLogicalPlan::Negative(expr) => {
                    let l = self.convert_to_expr(expr[0])?;
                    Expr::Negative(Box::new(l))
                }
                TokomakLogicalPlan::Between([expr, low, high]) => {
                    let expr = self.convert_to_expr(*expr)?;
                    let low_expr = self.convert_to_expr(*low)?;
                    let high_expr = self.convert_to_expr(*high)?;
                    Expr::Between {
                        expr: Box::new(expr),
                        negated: false,
                        low: Box::new(low_expr),
                        high: Box::new(high_expr),
                    }
                }
                TokomakLogicalPlan::BetweenInverted([expr, low, high]) => {
                    let expr = self.convert_to_expr(*expr)?;
                    let low_expr = self.convert_to_expr(*low)?;
                    let high_expr = self.convert_to_expr(*high)?;
                    Expr::Between {
                        expr: Box::new(expr),
                        negated: true,
                        low: Box::new(low_expr),
                        high: Box::new(high_expr),
                    }
                }
                TokomakLogicalPlan::Column(col) => Expr::Column(col.into()),
                TokomakLogicalPlan::Cast([expr, dt]) => {
                    let l = self.convert_to_expr(*expr)?;
                    let dt = self.extract_datatype(*dt, "TryCast")?;
                    Expr::Cast { expr: Box::new(l), data_type: dt}
                }
                TokomakLogicalPlan::InList(inlist)=>self.convert_inlist(inlist, false)?,
                TokomakLogicalPlan::NotInList(inlist)=>self.convert_inlist(inlist, true)?,
                TokomakLogicalPlan::Scalar(s) => Expr::Literal(s.clone().into()),
                TokomakLogicalPlan::ScalarBuiltinCall(func_call) => {
                    let fun = match self.get_ref(func_call.fun()){
                        TokomakLogicalPlan::ScalarBuiltin(f)=>f,
                        f => return Err(unexpected_expr(f, "ScalarBuiltinCall", "TokomakExpr::ScalarBuiltin")),
                    };
                    let args = self.extract_expr_list(func_call.args(), "ScalarBuiltinCall.args")?;
                    Expr::ScalarFunction{
                        fun:fun.clone(),
                        args,
                    }
                },
                TokomakLogicalPlan::TryCast([expr,dt]) => {
                    let l = self.convert_to_expr(*expr)?;
                    let dt = self.extract_datatype(*dt , "TryCast")?;
                    Expr::TryCast { expr: Box::new(l), data_type: dt}
                },
                TokomakLogicalPlan::ScalarUDFCall(func_call) => {
                    let args = self.extract_expr_list(func_call.args(), "ScalarUDFCall.args")?;
                    let name = match self.get_ref(func_call.fun()){
                        TokomakLogicalPlan::ScalarUDF(name)=>name.0,
                        _=> return Err(DataFusionError::Plan("Could not get SCalar udf name".to_string()),)
                    };
                    let fun = match self.egraph.analysis.sudf_registry.get(&name){
                        Some(s) => s.clone(),
                        None => return Err(DataFusionError::Internal(format!("Did not find the scalar UDF {} in the registry", name))),
                    };
                    Expr::ScalarUDF{
                        fun,
                        args
                    }
                },
                TokomakLogicalPlan::AggregateBuiltinCall(func_call) =>{
                    let args = self.extract_expr_list(func_call.args(), "AggregateBuiltinCall.args")?;
                    let fun = match self.get_ref(func_call.fun()){
                        TokomakLogicalPlan::AggregateBuiltin(f)=>f.clone(),
                        e => return Err(unexpected_expr(e, "AggregateBuiltinCall", "AggregateBuiltin")),
                    };
                    Expr::AggregateFunction{
                        fun,
                        args,
                        distinct: false
                    }
                },
                TokomakLogicalPlan::AggregateBuiltinDistinctCall(func_call) => {
                    let args = self.extract_expr_list(func_call.args(), "AggregateBuiltinDistinctCall.args")?;
                    let fun = match self.get_ref(func_call.fun()){
                        TokomakLogicalPlan::AggregateBuiltin(f)=>f.clone(),
                        e => return Err(unexpected_expr(e, "AggregateBuiltinDistinctCall.args", "TokomakExpr::AggregateBuiltin")),
                    };
                    Expr::AggregateFunction{
                        fun,
                        args,
                        distinct: true
                    }
                },
                TokomakLogicalPlan::AggregateUDFCall(func_call) => {
                    let udf_name = match self.get_ref(func_call.fun()){
                        TokomakLogicalPlan::AggregateUDF(name)=> name.0,
                        e =>    return Err(DataFusionError::Internal(format!("Expected an AggregateUDF node at index 0 of AggregateUDFCall, found: {:?}",e)))
                    };
                    let fun = match self.egraph.analysis.udaf_registry.get(&udf_name){
                        Some(s) => s.clone(),
                        None => return Err(DataFusionError::Internal(format!("Did not find the aggregate UDF '{}' in the registry", udf_name))),
                    };
                    let args = self.extract_expr_list(func_call.args(), "AggregateUDF.args")?;
                    Expr::AggregateUDF{
                        fun,
                        args
                    }
                },
                TokomakLogicalPlan::WindowBuiltinCallUnframed(call) => {
                    let fun = match self.get_ref(call.fun()){
                        TokomakLogicalPlan::WindowBuiltin(f)=>f.clone(),
                        node => return Err(unexpected_expr(node, "WindowBuiltinCallUnframed.fun", "TokomakExpr::WindowBuiltin")),
                    };
                    let partition_by = self.extract_expr_list(call.partition_list(), "WindowBuiltinCallUnframed")?;
                    let order_by = self.extract_expr_list(call.order_by_list(), "WindowBuiltinCallUnframed")?;
                    let args = self.extract_expr_list(call.args_list(), "WindowBuiltinCallUnframed")?;
                    Expr::WindowFunction{
                        fun,
                        partition_by,
                        order_by,
                        args,
                        window_frame: None
                    }
                },
                TokomakLogicalPlan::WindowBuiltinCallFramed(call) => {
                    let fun = match self.get_ref(call.fun()){
                        TokomakLogicalPlan::WindowBuiltin(f)=>f.clone(),
                        e=>return Err(unexpected_expr(e, "WindowBuiltinCallFramed", "TokomakExpr::WindowBuiltin")),
                    };
                    let partition_by = self.extract_expr_list(call.partition_list(), "WindowBuiltinCallFramed")?;
                    let order_by = self.extract_expr_list(call.order_by_list(), "WindowBuiltinCallFramed")?;
                    let args = self.extract_expr_list(call.args_list(), "WindowBuiltinCallFramed")?;

                    let window_frame = match self.get_ref(call.frame()){
                        TokomakLogicalPlan::WindowFrame(f)=>*f,
                        e=> return Err(unexpected_expr(e, "WindowBuiltinCallUnframed.frame", "TokomakExpr::WindowFrame")),
                    };
                    Expr::WindowFunction{
                        fun,
                        partition_by,
                        order_by,
                        args,
                        window_frame: Some(window_frame)
                    }
                },
                TokomakLogicalPlan::SortExpr(sort) => {
                    let expr = self.convert_to_expr(sort.expr())?;
                    let sort_spec = match self.get_ref(sort.sort_spec()){
                        TokomakLogicalPlan::SortSpec(s)=>*s,
                        e => return Err(unexpected_expr(e, "Sort.sort_spec", "TokomakExpr::SortSpec")),
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
                TokomakLogicalPlan::CaseIf(caseif)=>{
                    let expr =None;
                    let when_then_expr = self.to_when_then_list(caseif.when_then())?;
                    let else_expr = match caseif.else_expr(){
                        Some(c) => Some(self.convert_to_expr(c)?.into()),
                        None => None,
                    };
                    Expr::Case{expr, when_then_expr, else_expr}
                }
                TokomakLogicalPlan::CaseLit(caselit)=>{
                    let expr = self.convert_to_expr(caselit.expr())?.into();
                    let when_then = self.to_when_then_list(caselit.when_then())?;
                    println!("when_then: {:?}", when_then);
                    let else_expr = match caselit.else_expr(){
                        Some(c) => Some(self.convert_to_expr(c)?.into()),
                        None => None,
                    };
                    Expr::Case{expr: Some(expr), when_then_expr: when_then, else_expr}
                }
                unconvertible_expr@ (
                    TokomakLogicalPlan::AggregateBuiltin(_)|
                TokomakLogicalPlan::ScalarBuiltin(_) |
                TokomakLogicalPlan::AggregateUDF(_) |
                TokomakLogicalPlan::ScalarUDF(_) |
                 TokomakLogicalPlan::SortSpec(_) |
                 TokomakLogicalPlan::WindowFrame(_)|
                 TokomakLogicalPlan::WindowBuiltin(_)|
                  TokomakLogicalPlan::EList(_)|
                TokomakLogicalPlan::Type(_))=>return Err(DataFusionError::Internal(format!("The Expr {:?} should only ever be converted within a parent expression conversion. E.g. SortSpec conversion should be handled when converting Sort", unconvertible_expr))),
                plan @ (
                TokomakLogicalPlan::Hash(_)|
                TokomakLogicalPlan::RoundRobinBatch(_)|
                TokomakLogicalPlan::Filter(_)|
                TokomakLogicalPlan::Projection(_)|
                TokomakLogicalPlan::InnerJoin(_)|
                TokomakLogicalPlan::LeftJoin(_)|
                TokomakLogicalPlan::RightJoin(_)|
                TokomakLogicalPlan::FullJoin(_)|
                TokomakLogicalPlan::SemiJoin(_)|
                TokomakLogicalPlan::AntiJoin(_)|
                TokomakLogicalPlan::Extension(_)|
                TokomakLogicalPlan::CrossJoin(_)|
                TokomakLogicalPlan::Limit(_)|
                TokomakLogicalPlan::EmptyRelation(_)|
                TokomakLogicalPlan::TableScan(_)|
                TokomakLogicalPlan::Union(_)|
                TokomakLogicalPlan::Window(_)|
                TokomakLogicalPlan::Aggregate(_)|
                TokomakLogicalPlan::Sort(_)|
                TokomakLogicalPlan::PList(_)|
                TokomakLogicalPlan::Table(_)|
                TokomakLogicalPlan::Values(_)|
                TokomakLogicalPlan::TableProject(_)|
                TokomakLogicalPlan::JoinKeys(_)|
                TokomakLogicalPlan::Str(_)|
                TokomakLogicalPlan::None )=> return Err(DataFusionError::Internal(format!("Expected expression found plan: {}", plan)))
            };
        Ok(expr)
    }

    fn extract_plan_list(
        &self,
        id: Id,
        parent: &'static str,
    ) -> Result<Vec<LogicalPlanBuilder>, DataFusionError> {
        let list = match self.get_ref(id) {
            TokomakLogicalPlan::PList(plans) => plans,
            plan => return Err(unexpected_node(parent, plan, "PList")),
        };
        if list.is_empty() {
            return Ok(Vec::new());
        }
        let mut plans = Vec::with_capacity(list.len());
        for id in list.iter() {
            plans.push(self.convert_to_builder(*id)?);
        }
        Ok(plans)
    }

    fn extract_expr_list_remove_uneccesary_aliases(
        &self,
        id: Id,
        input_schema: &DFSchema,
        parent: &'static str,
    ) -> Result<Vec<Expr>, DataFusionError> {
        let mut exprs = self.extract_expr_list(id, parent)?;
        for expr in exprs.iter_mut() {
            if let Expr::Alias(e, s) = expr {
                let extracted = e.name(input_schema)?;
                println!("alias: {:?}, calculated: {}", s, extracted);
                if *s == extracted {
                    println!("Not using alias: {} as it is unecessary", s);
                    let mut expr_swap = Expr::Wildcard;
                    std::mem::swap(&mut expr_swap, e.as_mut());
                    *expr = expr_swap;
                }
            }
            println!("expr: {:?}", expr);
        }

        Ok(exprs)
    }

    fn extract_expr_list(
        &self,
        id: Id,
        parent: &'static str,
    ) -> Result<Vec<Expr>, DataFusionError> {
        let exprs = match self.get_ref(id) {
            TokomakLogicalPlan::EList(list) => {
                if parent == "Projection.expr" {
                    let mut conv_list = Vec::with_capacity(list.len());
                    for i in list.iter() {
                        let idx: usize = (*i).into();
                        let eclass_id = self.eclasses[idx];
                        conv_list.push(eclass_id);
                    }
                }
                list.iter()
                    .map(|i| self.convert_to_expr(*i))
                    .collect::<Result<Vec<_>, _>>()?
            }
            plan => return Err(unexpected_node(parent, plan, "EList")),
        };
        Ok(exprs)
    }
    fn extract_column(
        &self,
        id: Id,
        parent: &'static str,
    ) -> Result<Column, DataFusionError> {
        let col = match self.get_ref(id) {
            TokomakLogicalPlan::Column(c) => Column {
                relation: c.relation.as_ref().map(ToString::to_string),
                name: c.name.to_string(),
            },
            plan => return Err(unexpected_node(parent, plan, "TokomakExpr::Column")),
        };
        Ok(col)
    }
    fn convert_to_join(
        &self,
        join_type: JoinType,
        [left, right, on, null_equals_null]: &[Id; 4],
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let plan_name = match join_type {
            JoinType::Inner => "JoinType::Inner",
            JoinType::Left => "JoinType::Left",
            JoinType::Right => "JoinType::Right",
            JoinType::Full => "JoinType::Full",
            JoinType::Semi => "JoinType::Semi",
            JoinType::Anti => "JoinType::Anti",
        };
        let left = self.convert_to_builder(*left)?;
        let right = self.convert_to_builder(*right)?;
        let join_keys = match self.get_ref(*on) {
            TokomakLogicalPlan::JoinKeys(keys) => keys,
            plan => {
                return Err(DataFusionError::Internal(format!(
                    "The join of type {:?} expected a node of type JoinKeys, found {:?}",
                    join_type, plan
                )))
            }
        };

        let left_keys = match self.get_ref(join_keys[0]){
            TokomakLogicalPlan::EList(l)=> l,
            p => return Err(DataFusionError::Internal(format!("The join of type {:?} expected a node of type elist for the left join keys, found {:?}", join_type, p))),
        };
        let right_keys = match self.get_ref(join_keys[1]){
            TokomakLogicalPlan::EList(l)=> l,
            p => return Err(DataFusionError::Internal(format!("The join of type {:?} expected a node of type elist for the right join keys, found {:?}", join_type, p))),
        };
        let mut left_join_keys = Vec::with_capacity(left_keys.len());
        let mut right_join_keys = Vec::with_capacity(right_keys.len());
        for (left_key, right_key) in left_keys.iter().zip(right_keys.iter()) {
            left_join_keys.push(self.extract_column(*left_key, plan_name)?);
            right_join_keys.push(self.extract_column(*right_key, plan_name)?)
        }
        let null_equals_null = match self.get_ref(*null_equals_null) {
            TokomakLogicalPlan::Scalar(TokomakScalar::Boolean(Some(b))) => *b,
            p => {
                return Err(DataFusionError::Internal(format!(
                "The join of type {:?} expexted a non-null scalar boolean, found: {:?}",
                join_type, p
            )))
            }
        };
        left.join_detailed(
            &right.build()?,
            join_type,
            (left_join_keys, right_join_keys),
            null_equals_null,
        )
    }
    fn extract_alias(
        &self,
        id: Id,
        parent: &'static str,
    ) -> Result<Option<String>, DataFusionError> {
        let alias = match self.get_ref(id) {
            TokomakLogicalPlan::Str(sym) => Some(sym.to_string()),
            TokomakLogicalPlan::None => None,
            plan => return Err(unexpected_node(parent, plan, "Str or None")),
        };
        Ok(alias)
    }

    fn get_schema(&self, id: Id) -> Option<&DFSchema> {
        let eclass_id = self.get_eclass_id(id);
        match &self.egraph[eclass_id].data {
            TData::Schema(s) => Some(s),
            _ => None,
        }
    }
    fn get_eclass_id(&self, id: Id) -> Id {
        let eclass_idx: usize = id.into();

        self.eclasses[eclass_idx]
    }

    fn convert_to_builder(&self, id: Id) -> Result<LogicalPlanBuilder, DataFusionError> {
        let plan = self.get_ref(id);
        let builder: LogicalPlanBuilder = match plan {
            TokomakLogicalPlan::Filter([input, predicate]) => {
                let predicate = self.convert_to_expr(*predicate)?;
                let input = self.convert_to_builder(*input)?;
                input.filter(predicate)?
            }
            TokomakLogicalPlan::Projection([input, exprs, alias]) => {
                let input = self.convert_to_builder(*input)?;
                let alias = self.extract_alias(*alias, "Projection")?;
                let exprs = self.extract_expr_list_remove_uneccesary_aliases(
                    *exprs,
                    input.schema(),
                    "Projection.expr",
                )?;

                input.project_with_alias(exprs, alias)?
            }
            TokomakLogicalPlan::InnerJoin(join) => {
                self.convert_to_join(JoinType::Inner, join)?
            }
            TokomakLogicalPlan::LeftJoin(join) => {
                self.convert_to_join(JoinType::Left, join)?
            }
            TokomakLogicalPlan::RightJoin(join) => {
                self.convert_to_join(JoinType::Right, join)?
            }
            TokomakLogicalPlan::FullJoin(join) => {
                self.convert_to_join(JoinType::Full, join)?
            }
            TokomakLogicalPlan::SemiJoin(join) => {
                self.convert_to_join(JoinType::Semi, join)?
            }
            TokomakLogicalPlan::AntiJoin(join) => {
                self.convert_to_join(JoinType::Anti, join)?
            }
            TokomakLogicalPlan::Extension(_) => {
                return Err(DataFusionError::NotImplemented(String::from(
                    "TokomakOptimizer does not support extension nodes yet",
                )))
            }
            TokomakLogicalPlan::CrossJoin([left, right]) => {
                let left = self.convert_to_builder(*left)?;
                let right = self.convert_to_builder(*right)?.build()?;
                left.cross_join(&right)?
            }
            TokomakLogicalPlan::Limit(l) => {
                let input = self.convert_to_builder(l[0])?;
                let limit = self.extract_usize(l[1], "Limit")?;
                input.limit(limit)?
            }
            TokomakLogicalPlan::EmptyRelation(produce_one_row) => {
                let produce_one_row = match self.get_ref(produce_one_row[0]){
                    TokomakLogicalPlan::Scalar(TokomakScalar::Boolean(Some(val)))=>* val,
                    TokomakLogicalPlan::Scalar(non_boolean) => return Err(DataFusionError::Internal(format!("Expected a boolean scalar for EmptyRelation.produce_one_row found {:?}", non_boolean))),
                    plan =>return Err(unexpected_node("EmptyRelation.produce_one_row", plan, "Scalar"))
                };
                let schema = self.get_schema(id).ok_or_else(|| {
                    DataFusionError::Plan(
                        "Could not find schema for empty relation".to_string(),
                    )
                })?;
                LogicalPlanBuilder::from(LogicalPlan::EmptyRelation(
                    plan::EmptyRelation {
                        produce_one_row,
                        schema: Arc::new(schema.clone()),
                    },
                ))
            }
            TokomakLogicalPlan::TableScan(s) => {
                let name = match self.get_ref(s.name()) {
                    TokomakLogicalPlan::Str(sym) => sym.to_string(),
                    unexpected_plan => {
                        return Err(unexpected_node(
                            "TableScan.name",
                            unexpected_plan,
                            "Str",
                        ))
                    }
                };
                let limit = match self.get_ref(s.limit()) {
                    TokomakLogicalPlan::Scalar(s) => {
                        Some(self.extract_usize_from_scalar(s, "TableScan.limit")?)
                    }
                    TokomakLogicalPlan::None => None,
                    unexpected_plan => {
                        return Err(unexpected_node(
                            "TableScan.limit",
                            unexpected_plan,
                            "LimitCount or None ",
                        ))
                    }
                };
                let source = match self.get_ref(s.source()) {
                    TokomakLogicalPlan::Table(t) => t.0.clone(),
                    unexpected_plan => {
                        return Err(unexpected_node(
                            "TableScan.provider",
                            unexpected_plan,
                            "Table",
                        ))
                    }
                };
                let projection = match self.get_ref(s.projection()) {
                    TokomakLogicalPlan::None => None,
                    TokomakLogicalPlan::TableProject(proj) => {
                        Some(proj.0.as_ref().clone())
                    }
                    unexpected_plan => {
                        return Err(unexpected_node(
                            "TableScan.projection",
                            unexpected_plan,
                            "TableProject or None",
                        ))
                    }
                };
                let filters = self.extract_expr_list(s.filters(), "TableScan.filters")?;

                LogicalPlanBuilder::scan_with_limit_and_filters(
                    name, source, projection, limit, filters,
                )?
            }
            TokomakLogicalPlan::Hash([input, exprs, size]) => {
                let input = self.convert_to_builder(*input)?;
                let exprs = self.extract_expr_list(*exprs, "Repartition<Hash>.exprs")?;
                let size = self.extract_usize(*size, "Repartition<Hash>.size")?;
                input.repartition(Partitioning::Hash(exprs, size))?
            }
            TokomakLogicalPlan::RoundRobinBatch([input, size]) => {
                let input = self.convert_to_builder(*input)?;
                let size = self.extract_usize(*size, "Repartition<RoundRobin>.size")?;
                input.repartition(Partitioning::RoundRobinBatch(size))?
            }
            TokomakLogicalPlan::Union([inputs, alias]) => {
                let mut builders = self.extract_plan_list(*inputs, "Union.inputs")?;
                if builders.is_empty() {
                    return Err(DataFusionError::Internal(
                        "Union requires at least a single child plan, found 0"
                            .to_string(),
                    ));
                } else if builders.len() == 1 {
                    return Ok(builders.pop().unwrap());
                }
                let alias = self.extract_alias(*alias, "Union.alias")?;
                let mut builder = builders.pop().unwrap();
                let apply_alias = builders.pop().unwrap().build()?;
                for other in builders {
                    builder = builder.union(other.build()?)?;
                }
                builder.union_with_alias(apply_alias, alias)?
            }
            TokomakLogicalPlan::Window([input, window_exprs]) => {
                let input = self.convert_to_builder(*input)?;
                let window_expr = self.extract_expr_list_remove_uneccesary_aliases(
                    *window_exprs,
                    input.schema().as_ref(),
                    "Window.window_expr",
                )?;
                input.window(window_expr)?
            }
            TokomakLogicalPlan::Aggregate([input, aggr_expr, group_expr]) => {
                let input = self.convert_to_builder(*input)?;
                let aggr_expr = self.extract_expr_list_remove_uneccesary_aliases(
                    *aggr_expr,
                    input.schema().as_ref(),
                    "Aggregate.aggr_expr",
                )?;
                println!("Aggr expr{:?}", aggr_expr);
                let group_expr = self.extract_expr_list_remove_uneccesary_aliases(
                    *group_expr,
                    input.schema().as_ref(),
                    "Aggregate.group_expr",
                )?;
                let p = input.build()?;
                let input = LogicalPlanBuilder::from(p);
                input.aggregate(group_expr, aggr_expr)?
            }
            TokomakLogicalPlan::Sort([input, sort_exprs]) => {
                let input = self.convert_to_builder(*input)?;
                let exprs = self.extract_expr_list(*sort_exprs, "Sort.exprs")?;
                input.sort(exprs)?
            }
            TokomakLogicalPlan::Values(values)=>{
                let values = &values.0;
                let schema = if let TData::Schema(schema) = &self.egraph[id].data{
                    schema.as_ref().clone()
                }else{
                    return Err(DataFusionError::Internal(format!("Found egraph data: {:?} expected Schema", self.egraph[id].data)));
                };
                let plan = LogicalPlan::Values(Values{
                    values: values.as_ref().clone(),
                    schema: Arc::new(schema)
                });
                LogicalPlanBuilder::from(plan)
            }
            handle_elsewhere @ (TokomakLogicalPlan::Table(_)
            | TokomakLogicalPlan::TableProject(_)
            | TokomakLogicalPlan::Str(_)
            | TokomakLogicalPlan::None
            | TokomakLogicalPlan::JoinKeys(_)
            | TokomakLogicalPlan::PList(_)
            | TokomakLogicalPlan::Type(_)) => {
                return Err(DataFusionError::Internal(format!(
                    "Found unhandled node type {:?}. This should have been handled by the parent plan",
                    handle_elsewhere
                )))
            }

            expr @ (TokomakLogicalPlan::ExprAlias(_)
            | TokomakLogicalPlan::Plus(_)
            | TokomakLogicalPlan::Minus(_)
            | TokomakLogicalPlan::Multiply(_)
            | TokomakLogicalPlan::Divide(_)
            | TokomakLogicalPlan::Modulus(_)
            | TokomakLogicalPlan::Or(_)
            | TokomakLogicalPlan::And(_)
            | TokomakLogicalPlan::Eq(_)
            | TokomakLogicalPlan::NotEq(_)
            | TokomakLogicalPlan::Lt(_)
            | TokomakLogicalPlan::LtEq(_)
            | TokomakLogicalPlan::Gt(_)
            | TokomakLogicalPlan::GtEq(_)
            | TokomakLogicalPlan::RegexMatch(_)
            | TokomakLogicalPlan::RegexIMatch(_)
            | TokomakLogicalPlan::RegexNotMatch(_)
            | TokomakLogicalPlan::RegexNotIMatch(_)
            | TokomakLogicalPlan::IsDistinctFrom(_)
            | TokomakLogicalPlan::IsNotDistinctFrom(_)
            | TokomakLogicalPlan::Not(_)
            | TokomakLogicalPlan::IsNotNull(_)
            | TokomakLogicalPlan::IsNull(_)
            | TokomakLogicalPlan::Negative(_)
            | TokomakLogicalPlan::Between(_)
            | TokomakLogicalPlan::BetweenInverted(_)
            | TokomakLogicalPlan::Like(_)
            | TokomakLogicalPlan::NotLike(_)
            | TokomakLogicalPlan::InList(_)
            | TokomakLogicalPlan::NotInList(_)
            | TokomakLogicalPlan::EList(_)
            | TokomakLogicalPlan::ScalarBuiltin(_)
            | TokomakLogicalPlan::AggregateBuiltin(_)
            | TokomakLogicalPlan::WindowBuiltin(_)
            | TokomakLogicalPlan::Scalar(_)
            | TokomakLogicalPlan::ScalarBuiltinCall(_)
            | TokomakLogicalPlan::ScalarUDFCall(_)
            | TokomakLogicalPlan::AggregateBuiltinCall(_)
            | TokomakLogicalPlan::AggregateBuiltinDistinctCall(_)
            | TokomakLogicalPlan::AggregateUDFCall(_)
            | TokomakLogicalPlan::WindowBuiltinCallUnframed(_)
            | TokomakLogicalPlan::WindowBuiltinCallFramed(_)
            | TokomakLogicalPlan::SortExpr(_)
            | TokomakLogicalPlan::SortSpec(_)
            | TokomakLogicalPlan::ScalarUDF(_)
            | TokomakLogicalPlan::AggregateUDF(_)
            | TokomakLogicalPlan::Column(_)
            | TokomakLogicalPlan::WindowFrame(_)
            | TokomakLogicalPlan::Cast(_)
            | TokomakLogicalPlan::TryCast(_)
            | TokomakLogicalPlan::CaseLit(_)
            | TokomakLogicalPlan::CaseIf(_)) => {
                return Err(DataFusionError::Internal(format!(
                    "Expected plan found expression: {}",
                    expr
                )))
            }
        };
        Ok(builder)
    }
}

pub(crate) fn convert_to_df_plan(
    plan: &RecExpr<TokomakLogicalPlan>,
    eclasses: &[Id],
    egraph: &EGraph<TokomakLogicalPlan, TokomakAnalysis>,
) -> Result<LogicalPlan, DataFusionError> {
    let converter = TokomakPlanConverter::new(plan, eclasses, egraph);
    let start = plan.as_ref().len() - 1;
    converter.convert_to_builder(start.into())?.build()
}
#[derive(Debug, Clone)]
///Tokomak compatible wrapper around Values
pub struct SharedValues(pub Arc<Vec<Vec<Expr>>>);
impl Eq for SharedValues {}
impl PartialEq for SharedValues {
    fn eq(&self, other: &Self) -> bool {
        for (srow, orow) in self.0.iter().zip(other.0.as_slice()) {
            for (sc, oc) in srow.iter().zip(orow) {
                if sc != oc {
                    return false;
                }
            }
        }
        true
    }
}

impl Hash for SharedValues {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let num_exprs = self.0.iter().fold(0usize, |acc, value| value.len() + acc);
        let mut buf = String::with_capacity(10 * num_exprs);
        for e in self.0.iter().flatten() {
            buf.clear();
            write!(buf, "{:?}\"'", e).unwrap();
            buf.hash(state);
        }
    }
}

impl FromStr for SharedValues {
    type Err = DataFusionError;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Err(DataFusionError::NotImplemented(
            "Parsing shared values is not supported for SharedValues".to_owned(),
        ))
    }
}
impl Ord for SharedValues {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
impl PartialOrd for SharedValues {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        for (srow, orow) in self.0.iter().zip(other.0.as_slice()) {
            for (sc, oc) in srow.iter().zip(orow) {
                if let Some(ord) = sc.partial_cmp(oc) {
                    if Ordering::Equal != ord {
                        return Some(ord);
                    }
                }
            }
        }
        Some(Ordering::Equal)
    }
}

impl std::fmt::Display for SharedValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VALUES({})", self.0.len())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        datasource::empty::EmptyTable,
        error::DataFusionError,
        execution::context::ExecutionProps,
        logical_plan::{
            col, count, lit, sum,
            window_frames::{WindowFrame, WindowFrameBound, WindowFrameUnits},
            Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Partitioning,
        },
        optimizer::optimizer::OptimizerRule,
        physical_plan::window_functions::{BuiltInWindowFunction, WindowFunction},
    };
    use egg::{EGraph, Extractor};

    fn plan_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Boolean, true),
            Field::new("c3", DataType::Float64, true),
            Field::new("c1_non_null", DataType::Utf8, false),
            Field::new("c2_non_null", DataType::Boolean, false),
            Field::new("c3_non_null", DataType::Float64, false),
        ])
    }
    type TestResult = Result<(), Box<dyn std::error::Error>>;
    fn roundtrip_plan(plan: &LogicalPlan) -> TestResult {
        let mut egraph = EGraph::default();
        let root = crate::plan::to_tokomak_plan(plan, &mut egraph)?;
        let ex = Extractor::new(&egraph, crate::DefaultCostFunc);
        let (_, rec_expr_plan) = ex.find_best(root);
        let eclass_ids = egraph.lookup_expr_ids(&rec_expr_plan).ok_or_else(|| {
            DataFusionError::Internal(String::from(
                "TokomakOptimizer could not extract plan from egraph.",
            ))
        })?;
        let rt_plan =
            crate::plan::convert_to_df_plan(&rec_expr_plan, &eclass_ids, &egraph)?;
        assert_eq!(format!("{:?}", plan), format!("{:?}", rt_plan));
        Ok(())
    }
    fn roundtrip_plans(plans: &[LogicalPlan]) -> TestResult {
        for plan in plans {
            roundtrip_plan(plan)?;
        }
        Ok(())
    }

    fn roundtrip_plan_builders(builders: &[LogicalPlanBuilder]) -> TestResult {
        for b in builders {
            let plan = b.build()?;
            roundtrip_plan(&plan)?;
        }
        Ok(())
    }

    fn plan_test_scan_builder() -> LogicalPlanBuilder {
        LogicalPlanBuilder::scan_empty(Some("test"), &plan_test_schema(), None).unwrap()
    }
    #[test]
    fn roundtrip_projection() -> TestResult {
        let b = plan_test_scan_builder();
        println!("{:?}", b.build()?);
        let builders = vec![
            b.project(vec![col("c3") * col("c3").alias("square"), col("c2")])?,
            b.project_with_alias(
                vec![col("c3") * col("c3").alias("square"), col("c2")],
                Some("test2".to_string()),
            )?,
        ];
        roundtrip_plan_builders(&builders)
    }

    fn make_empty_scan(
        table_name: &str,
        projection: Option<Vec<usize>>,
        filters: Option<Vec<Expr>>,
        limit: Option<usize>,
    ) -> Result<LogicalPlan, DataFusionError> {
        let table_schema = plan_test_schema();
        let source = Arc::new(EmptyTable::new(table_schema.into()));
        let builder = LogicalPlanBuilder::scan_with_limit_and_filters(
            table_name,
            source,
            projection,
            limit,
            filters.unwrap_or_default(),
        )?;
        builder.build()
    }

    #[test]
    fn roundtrip_scan() -> TestResult {
        let plans = vec![
            make_empty_scan("test", None, None, None)?,
            make_empty_scan("test", Some(vec![0, 1]), None, None)?,
            make_empty_scan("test", None, Some(vec![col("c1").eq(lit("test"))]), None)?,
            make_empty_scan("test", None, None, Some(12))?,
            make_empty_scan(
                "test",
                Some(vec![5, 4]),
                Some(vec![col("c2_non_null").eq(lit(false))]),
                None,
            )?,
            make_empty_scan("test", Some(vec![5, 4]), None, Some(35))?,
            make_empty_scan(
                "test",
                Some(vec![5, 4]),
                Some(vec![col("c2_non_null").eq(lit(false))]),
                Some(usize::MAX),
            )?,
        ];
        roundtrip_plans(&plans)
    }

    fn create_optimizer() -> crate::Tokomak {
        crate::Tokomak::with_builtin_rules(
            crate::RunnerSettings::default(),
            crate::ALL_RULES,
        )
    }
    //May need to remove this test as filter expressions add expression to predicate.
    //No gaurentee of determinism with regards to the extracted plan. Expressions may be reorderd
    #[test]
    fn roundtrip_filter() -> TestResult {
        let b = plan_test_scan_builder();
        let builders = vec![
            b.filter(col("c1").eq(lit("test")))?,
            b.filter(
                col("c3")
                    .eq(lit(12.0f64))
                    .or(col("c1").like(lit("prefix%"))),
            )?,
        ];

        let optimizer = create_optimizer();
        for b in builders {
            let plan = b.build()?;
            let opt_plan = optimizer.optimize(&plan, &ExecutionProps::new())?;
            let fmt_plan = format!("{:?}", plan);
            let fmt_opt_plan = format!("{:?}", opt_plan);
            assert_eq!(fmt_plan, fmt_opt_plan);
        }
        Ok(())
    }

    #[test]
    fn roundtrip_joins() -> TestResult {
        let l = LogicalPlanBuilder::scan_empty(
            Some("left"),
            &Schema::new(vec![
                Field::new("lkey", DataType::Int64, false),
                Field::new("lkey2", DataType::Utf8, false),
                Field::new("ldata", DataType::Utf8, true),
            ]),
            None,
        )?;
        let r = LogicalPlanBuilder::scan_empty(
            Some("right"),
            &Schema::new(vec![
                Field::new("rkey", DataType::Int64, false),
                Field::new("rkey2", DataType::Utf8, false),
                Field::new("rdata", DataType::Utf8, true),
            ]),
            None,
        )?
        .build()?;

        let plans = vec![
            l.join(&r, JoinType::Inner, (vec!["lkey"], vec!["rkey"]))?,
            l.join(&r, JoinType::Left, (vec!["lkey"], vec!["rkey"]))?,
            l.join(&r, JoinType::Right, (vec!["lkey"], vec!["rkey"]))?,
            l.join(&r, JoinType::Anti, (vec!["lkey"], vec!["rkey"]))?,
            l.join(&r, JoinType::Semi, (vec!["lkey"], vec!["rkey"]))?,
            l.join_detailed(&r, JoinType::Inner, (vec!["lkey"], vec!["rkey"]), true)?,
            l.join_detailed(&r, JoinType::Left, (vec!["lkey"], vec!["rkey"]), true)?,
            l.join_detailed(&r, JoinType::Right, (vec!["lkey"], vec!["rkey"]), true)?,
            l.join_detailed(&r, JoinType::Anti, (vec!["lkey"], vec!["rkey"]), true)?,
            l.join_detailed(&r, JoinType::Semi, (vec!["lkey"], vec!["rkey"]), true)?,
            l.join(
                &r,
                JoinType::Inner,
                (vec!["lkey", "lkey2"], vec!["rkey", "rkey2"]),
            )?,
            l.join(
                &r,
                JoinType::Left,
                (vec!["lkey", "lkey2"], vec!["rkey", "rkey2"]),
            )?,
            l.join(
                &r,
                JoinType::Right,
                (vec!["lkey", "lkey2"], vec!["rkey", "rkey2"]),
            )?,
            l.join(
                &r,
                JoinType::Anti,
                (vec!["lkey", "lkey2"], vec!["rkey", "rkey2"]),
            )?,
            l.join(
                &r,
                JoinType::Semi,
                (vec!["lkey", "lkey2"], vec!["rkey", "rkey2"]),
            )?,
            l.join_detailed(
                &r,
                JoinType::Inner,
                (vec!["lkey", "lkey2"], vec!["rkey", "rkey2"]),
                true,
            )?,
            l.join_detailed(
                &r,
                JoinType::Left,
                (vec!["lkey", "lkey2"], vec!["rkey", "rkey2"]),
                true,
            )?,
            l.join_detailed(
                &r,
                JoinType::Right,
                (vec!["lkey", "lkey2"], vec!["rkey", "rkey2"]),
                true,
            )?,
            l.join_detailed(
                &r,
                JoinType::Anti,
                (vec!["lkey", "lkey2"], vec!["rkey", "rkey2"]),
                true,
            )?,
            l.join_detailed(
                &r,
                JoinType::Semi,
                (vec!["lkey", "lkey2"], vec!["rkey", "rkey2"]),
                true,
            )?,
        ];

        roundtrip_plan_builders(&plans)
    }
    #[test]
    fn roundtrip_limit() -> TestResult {
        let b = plan_test_scan_builder();
        let builders = vec![
            b.limit(0)?,
            b.limit(1)?,
            b.limit(12353241)?,
            b.limit(1235135)?,
            b.limit(11235)?,
            b.limit(932)?,
        ];
        roundtrip_plan_builders(&builders)
    }

    #[test]
    fn roundtrip_sort() -> TestResult {
        let b = plan_test_scan_builder();
        let builders = vec![
            b.sort(vec![
                col("c1").sort(false, false),
                col("c3").sort(false, true),
            ])?,
            b.sort(vec![
                col("c1").sort(false, false),
                col("c3").sort(true, false),
            ])?,
            b.sort(vec![
                col("c1").sort(false, true),
                col("c3").sort(false, false),
            ])?,
            b.sort(vec![
                col("c1").sort(true, false),
                col("c3").sort(false, false),
            ])?,
            b.sort(vec![
                col("c1").sort(false, false),
                col("c3").sort(true, true),
            ])?,
            b.sort(vec![
                col("c1").sort(false, true),
                col("c3").sort(false, false),
            ])?,
            b.sort(vec![
                col("c1").sort(true, false),
                col("c3").sort(false, true),
            ])?,
            b.sort(vec![
                col("c1").sort(false, true),
                col("c3").sort(true, true),
            ])?,
            b.sort(vec![
                col("c1").sort(true, false),
                col("c3").sort(true, true),
            ])?,
            b.sort(vec![
                col("c1").sort(false, true),
                col("c3").sort(true, true),
            ])?,
            b.sort(vec![col("c1").sort(true, true), col("c3").sort(true, true)])?,
        ];

        roundtrip_plan_builders(&builders)
    }

    #[test]
    fn roundtrip_repartition() -> TestResult {
        let b = plan_test_scan_builder();
        //Hash(Vec<Expr>, usize),
        let builders = vec![
            b.repartition(Partitioning::RoundRobinBatch(12))?,
            b.repartition(Partitioning::RoundRobinBatch(72))?,
            b.repartition(Partitioning::RoundRobinBatch(8192))?,
            b.repartition(Partitioning::Hash(
                vec![col("c1"), col("c2"), col("c3")],
                12,
            ))?,
            b.repartition(Partitioning::Hash(vec![col("c1"), col("c2")], 36))?,
            b.repartition(Partitioning::Hash(vec![col("c1")], 8192))?,
        ];
        roundtrip_plan_builders(&builders)
    }

    #[test]
    fn roudtrip_window() -> TestResult {
        let b = plan_test_scan_builder();
        let nth =
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::PercentRank);
        let builders = vec![
            b.window(vec![count(col("c2")), sum(col("c3"))])?,
            b.window(vec![Expr::WindowFunction {
                fun: nth.clone(),
                args: vec![],
                partition_by: vec![col("c1")],
                order_by: vec![col("c2")],
                window_frame: None,
            }])?,
            b.window(vec![Expr::WindowFunction {
                fun: nth,
                args: vec![],
                partition_by: vec![col("c1")],
                order_by: vec![],
                window_frame: Some(WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::CurrentRow,
                    end_bound: WindowFrameBound::CurrentRow,
                }),
            }])?,
        ];
        roundtrip_plan_builders(&builders)
    }
}
