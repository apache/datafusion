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
#![deny(missing_docs)]

//! This crate contains the equality graph optimizer Tokomak. It uses [egg](https://github.com/egraphs-good/egg).

use datafusion::arrow::datatypes::DataType;

use datafusion::physical_plan::aggregates::return_type as aggregate_return_type;
use datafusion::physical_plan::expressions::binary_operator_data_type;
use datafusion::physical_plan::functions::return_type as scalar_return_type;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::window_functions::return_type as window_return_type;

use datafusion::{logical_plan::LogicalPlan, optimizer::optimizer::OptimizerRule};

use log::info;
use plan::{convert_to_df_plan, CaseIf, CaseLit, TokomakLogicalPlan};

use std::fmt::Display;
use std::hash::Hash;

use std::str::FromStr;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::Operator;
use datafusion::logical_plan::{build_join_schema, DFField, DFSchema, JoinType};
use std::collections::HashMap;
use std::sync::Arc;

use egg::*;

pub mod datatype;
pub mod expr;
pub mod pattern;
pub mod plan;
mod rules;
pub use rules::utils;
pub mod scalar;

use datatype::TokomakDataType;

use std::collections::HashSet;

///TokomakAnalysis contains datatype and schema caches as well as the udf and udaf registries
pub struct TokomakAnalysis {
    datatype_cache: HashSet<RefCount<DataType>, fxhash::FxBuildHasher>,
    boolean_dt: RefCount<DataType>,
    schema_cache: HashSet<RefCount<DFSchema>, fxhash::FxBuildHasher>,
    sudf_registry: HashMap<Symbol, Arc<ScalarUDF>>,
    udaf_registry: HashMap<Symbol, Arc<AggregateUDF>>,
    always_merge: bool,
}
impl Default for TokomakAnalysis {
    fn default() -> Self {
        let mut a = Self {
            datatype_cache: Default::default(),
            schema_cache: Default::default(),
            sudf_registry: Default::default(),
            udaf_registry: Default::default(),
            always_merge: true,
            boolean_dt: Arc::new(DataType::Boolean),
        };
        a.datatype_cache.insert(Arc::new(DataType::Boolean));
        a
    }
}

//TODO: Check if Rc would work instead of Arc
type RefCount<T> = Arc<T>;

#[derive(Debug, Clone)]
///Each EClass in the EGraph is assigned a TData. Only supporting nodes such as EList should be marked as None.
/// All other nodes should be able do derive their data from their children's data.
pub enum TData {
    ///Holds a reference counted DataType.
    DataType(RefCount<DataType>),
    ///Reference counted schema with the fields sorted by name. Never use the column index from these schemas and never use the nullability information
    /// in the schema in an optimization rule as it is unsound.
    Schema(RefCount<DFSchema>),
    ///This should only be assigned to supporting nodes such as EList or PList.
    None,
}

impl TokomakAnalysis {
    fn get_boolean(egraph: &EGraph<TokomakLogicalPlan, Self>) -> TData {
        TData::DataType(egraph.analysis.boolean_dt.clone())
    }
    fn get_data(egraph: &EGraph<TokomakLogicalPlan, Self>, id: &Id) -> TData {
        egraph[*id].data.clone()
    }
    fn get_datatype(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        id: &Id,
    ) -> Option<RefCount<DataType>> {
        match Self::get_data(egraph, id) {
            TData::Schema(_) | TData::None => None,
            TData::DataType(dt) => Some(dt),
        }
    }

    fn get_schema(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        id: Id,
    ) -> Option<RefCount<DFSchema>> {
        match Self::get_data(egraph, &id) {
            TData::DataType(_) | TData::None => None,
            TData::Schema(s) => Some(s),
        }
    }
    fn get_projection(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        [_input, exprs, alias]: &[Id; 3],
    ) -> TData {
        assert!(
            egraph[*exprs].nodes.len() == 1,
            "EList must have a single representation found {}",
            egraph[*exprs].nodes.len()
        );
        let elist = match &egraph[*exprs].nodes[0] {
            TokomakLogicalPlan::EList(list) => list,
            p => panic!("Projection expected EList found {}", p),
        };
        assert!(
            egraph[*alias].nodes.len() == 1,
            "Alias must have a sinlge representation, found: {}",
            egraph[*alias].nodes.len()
        );
        let alias = match &egraph[*alias].nodes[0] {
            TokomakLogicalPlan::Str(s) => Some(s.as_str()),
            TokomakLogicalPlan::None => None,
            p => panic!("Projections expected Str or None as alias found {}", p),
        };
        let mut fields = Vec::with_capacity(elist.len());
        for expr_id in elist.iter() {
            let (datatype, name) = match &egraph[*expr_id].nodes[0]{
                TokomakLogicalPlan::ExprAlias([e_id, a_id])=>{
                    let dt = Self::get_datatype(egraph, e_id).expect("Projection could not find expressions datatype");
                    assert!(egraph[*a_id].nodes.len()==1);
                    let alias = match &egraph[*a_id].nodes[0]{
                        TokomakLogicalPlan::Str(s)=>s,
                        p=> panic!("ExprAlias expected an Str found {}", p),
                    };
                    (dt, *alias)
                },
                TokomakLogicalPlan::Column(c)=>{
                    let dt = egraph.lookup(TokomakLogicalPlan::Column(c.clone())).unwrap();
                    let dt = Self::get_datatype(egraph, &dt).unwrap();
                    let name = c.name;
                    (dt, name)
                }
                e => panic!("Projection's top level expressions must always be aliased to preserve naming, found {}", e),
            };
            let field =
                DFField::new(alias, name.as_str(), datatype.as_ref().clone(), true);
            fields.push(field);
        }
        fields.sort_by_key(|f| f.qualified_name());
        let s = DFSchema::new(fields).expect("Projection could not build schema");
        let s = Self::check_schema_cache(egraph, s);
        TData::Schema(s)
    }
    fn make_aggregate(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        [_input, aggr_expr, _group_expr]: &[Id; 3],
    ) -> TData {
        assert!(egraph[*aggr_expr].nodes.len() == 1);
        let aggr_exprs = match &egraph[*aggr_expr].nodes[0] {
            TokomakLogicalPlan::EList(list) => list,
            p => panic!(
                "Aggregate expected EList for aggregate expresssions found: {}",
                p
            ),
        };
        let mut fields = Vec::with_capacity(aggr_exprs.len());
        for expr_id in aggr_exprs.iter() {
            let (datatype, name) = match &egraph[*expr_id].nodes[0]{
                TokomakLogicalPlan::ExprAlias([e_id, a_id])=>{
                    let dt = Self::get_datatype(egraph, e_id).expect("Aggregate could not find expressions datatype");
                    assert!(egraph[*a_id].nodes.len()==1);
                    let alias = match &egraph[*a_id].nodes[0]{
                        TokomakLogicalPlan::Str(s)=>s,
                        p=> panic!("ExprAlias expected an Str found {}", p),
                    };
                    (dt, *alias)
                },
                TokomakLogicalPlan::Column(c)=>{
                    let dt = egraph.lookup(TokomakLogicalPlan::Column(c.clone())).unwrap();
                    let dt = Self::get_datatype(egraph, &dt).unwrap();
                    let name = c.name;
                    (dt, name)
                }
                e => panic!("Aggregate's top level expressions must always be aliased to preserve naming, found {}", e),
            };
            let field =
                DFField::new(None, name.as_str(), datatype.as_ref().clone(), true);
            fields.push(field);
        }
        fields.sort_by_key(|f| f.qualified_name());
        let schema = DFSchema::new(fields).unwrap();
        let s = Self::check_schema_cache(egraph, schema);
        TData::Schema(s)
    }

    fn make_binop(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        op: Operator,
        [l, r]: &[Id; 2],
    ) -> TData {
        let ldt = Self::get_datatype(egraph, l)
            .unwrap_or_else(|| panic!("Binop {} could not get left datatype", op));
        let rdt = Self::get_datatype(egraph, r)
            .unwrap_or_else(|| panic!("Binop {} could not get right datatype", op));
        let dt = binary_operator_data_type(&ldt, &op, &rdt).unwrap();
        TData::DataType(Self::check_datatype_cache(egraph, dt))
    }

    fn check_datatype_cache(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        dt: DataType,
    ) -> RefCount<DataType> {
        match egraph.analysis.datatype_cache.get(&dt) {
            Some(s) => s.clone(),
            None => RefCount::new(dt),
        }
    }

    fn check_schema_cache(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        schema: DFSchema,
    ) -> RefCount<DFSchema> {
        match egraph.analysis.schema_cache.get(&schema) {
            Some(s) => s.clone(),
            None => RefCount::new(schema),
        }
    }

    fn get_elist_datatypes(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        id: &Id,
    ) -> Option<Vec<DataType>> {
        assert!(egraph[*id].nodes.len() == 1);
        let id_list = match &egraph[*id].nodes[0] {
            TokomakLogicalPlan::EList(list) => list,
            _ => return None,
        };
        let dt_list = id_list
            .iter()
            .map(|id| Self::get_datatype(egraph, id).map(|d| d.as_ref().clone()))
            .collect::<Option<Vec<_>>>()?;
        Some(dt_list)
    }

    fn make_join(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        [l, r, _keys, _null_equals_null]: &[Id; 4],
        join_type: JoinType,
    ) -> TData {
        let lschema = Self::get_schema(egraph, *l).unwrap();
        let rschema = Self::get_schema(egraph, *r).unwrap();
        let s = build_join_schema(&lschema, &rschema, &join_type).unwrap();
        let mut fields = s.fields().clone();
        fields.sort_by_key(|f| f.qualified_name());
        let s = DFSchema::new(fields).unwrap();
        TData::Schema(Self::check_schema_cache(egraph, s))
    }

    fn make_window(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        [_input, window_exprs]: &[Id; 2],
    ) -> TData {
        assert!(egraph[*window_exprs].nodes.len() == 1);
        let aggr_exprs = match &egraph[*window_exprs].nodes[0] {
            TokomakLogicalPlan::EList(list) => list,
            p => panic!("Window expected EList for window expresssions found: {}", p),
        };
        let mut fields = Vec::with_capacity(aggr_exprs.len());
        for expr_id in aggr_exprs.iter() {
            let (datatype, name) = match &egraph[*expr_id].nodes[0]{
                TokomakLogicalPlan::ExprAlias([e_id, a_id])=>{
                    let dt = Self::get_datatype(egraph, e_id).expect("Window could not find expressions datatype");
                    assert!(egraph[*a_id].nodes.len()==1);
                    let alias = match &egraph[*a_id].nodes[0]{
                        TokomakLogicalPlan::Str(s)=>s,
                        p=> panic!("ExprAlias expected an Str found {}", p),
                    };
                    (dt, *alias)
                },
                TokomakLogicalPlan::Column(c)=>{
                    let dt = egraph.lookup(TokomakLogicalPlan::Column(c.clone())).unwrap();
                    let dt = Self::get_datatype(egraph, &dt).unwrap();
                    let name = c.name;
                    (dt, name)
                }
                e => panic!("Windows's top level expressions must always be aliased to preserve naming, found {}", e),
            };
            let field =
                DFField::new(None, name.as_str(), datatype.as_ref().clone(), true);
            fields.push(field);
        }
        fields.sort_by_key(|f| f.qualified_name());
        let schema = DFSchema::new(fields).unwrap();
        let s = Self::check_schema_cache(egraph, schema);
        TData::Schema(s)
    }

    fn make_case_lit(
        _egraph: &EGraph<TokomakLogicalPlan, Self>,
        _case: &CaseLit,
    ) -> TData {
        TData::None
    }
    fn make_case_if(_egraph: &EGraph<TokomakLogicalPlan, Self>, _case: &CaseIf) -> TData {
        TData::None
    }
    fn make_impl(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        enode: &TokomakLogicalPlan,
    ) -> <Self as Analysis<TokomakLogicalPlan>>::Data {
        let make_binop = |op: Operator, binop: &[Id; 2]| -> TData {
            Self::make_binop(egraph, op, binop)
        };
        let get_data = |id: &Id| -> TData { Self::get_data(egraph, id) };
        let data = match enode {
            TokomakLogicalPlan::Filter([input, _]) => Self::get_data(egraph, input),
            TokomakLogicalPlan::Projection(projection) => {
                Self::get_projection(egraph, projection)
            }
            TokomakLogicalPlan::InnerJoin(j) => {
                Self::make_join(egraph, j, JoinType::Inner)
            }
            TokomakLogicalPlan::LeftJoin(j) => Self::make_join(egraph, j, JoinType::Left),
            TokomakLogicalPlan::RightJoin(j) => {
                Self::make_join(egraph, j, JoinType::Right)
            }
            TokomakLogicalPlan::FullJoin(j) => Self::make_join(egraph, j, JoinType::Full),
            TokomakLogicalPlan::SemiJoin(j) => Self::make_join(egraph, j, JoinType::Semi),
            TokomakLogicalPlan::AntiJoin(j) => Self::make_join(egraph, j, JoinType::Anti),
            TokomakLogicalPlan::Extension(_) => todo!(),
            TokomakLogicalPlan::CrossJoin([l, r]) => {
                let lschema = Self::get_schema(egraph, *l).unwrap();
                let rschema = Self::get_schema(egraph, *r).unwrap();
                let cschema = lschema.join(rschema.as_ref()).unwrap();
                let mut fields = cschema.fields().clone();
                fields.sort_by_key(|f| f.qualified_name());
                let cschema = DFSchema::new(fields).unwrap();
                TData::Schema(Self::check_schema_cache(egraph, cschema))
            }
            TokomakLogicalPlan::Limit([input, _count]) => get_data(input),
            TokomakLogicalPlan::EmptyRelation(_) => TData::None,
            TokomakLogicalPlan::TableScan(t) => {
                let schema = t.projected_schema(egraph);

                TData::Schema(Self::check_schema_cache(egraph, schema))
            }
            TokomakLogicalPlan::Union([inputs, _alias]) => {
                assert!(egraph[*inputs].nodes.len() == 1);
                let plan_list = match &egraph[*inputs].nodes[0] {
                    TokomakLogicalPlan::PList(list) => list,
                    p => panic!("Union expected PList found {}", p),
                };
                for i in plan_list.iter() {
                    if let TData::Schema(s) = get_data(i) {
                        return TData::Schema(s);
                    }
                }
                TData::None
            }
            TokomakLogicalPlan::Window(window) => Self::make_window(egraph, window),
            TokomakLogicalPlan::Aggregate(agg) => Self::make_aggregate(egraph, agg),
            TokomakLogicalPlan::Hash([input, _exprs, _size]) => {
                Self::get_data(egraph, input)
            }
            TokomakLogicalPlan::RoundRobinBatch([input, _size]) => get_data(input),
            TokomakLogicalPlan::Sort([input, _sort_exprs]) => get_data(input),

            TokomakLogicalPlan::Plus(b) => make_binop(Operator::Plus, b),
            TokomakLogicalPlan::Minus(b) => make_binop(Operator::Minus, b),
            TokomakLogicalPlan::Multiply(b) => make_binop(Operator::Multiply, b),
            TokomakLogicalPlan::Divide(b) => make_binop(Operator::Divide, b),
            TokomakLogicalPlan::Modulus(b) => make_binop(Operator::Modulo, b),
            TokomakLogicalPlan::Or(_)
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
            | TokomakLogicalPlan::Between(_)
            | TokomakLogicalPlan::BetweenInverted(_)
            | TokomakLogicalPlan::Like(_)
            | TokomakLogicalPlan::NotLike(_)
            | TokomakLogicalPlan::InList(_)
            | TokomakLogicalPlan::NotInList(_) => Self::get_boolean(egraph),
            TokomakLogicalPlan::Negative([input]) => get_data(input),

            TokomakLogicalPlan::EList(_) => TData::None,

            TokomakLogicalPlan::Scalar(s) => {
                let dt = s.datatype();
                TData::DataType(RefCount::new(dt))
            }
            TokomakLogicalPlan::ScalarBuiltinCall(f) => {
                assert!(egraph[f.fun()].nodes.len() == 1);
                let builtin = match &egraph[f.fun()].nodes[0] {
                    TokomakLogicalPlan::ScalarBuiltin(b) => b,
                    _ => return TData::None,
                };
                let arg_datatypes = Self::get_elist_datatypes(egraph, &f.args());
                let arg_datatypes = match arg_datatypes {
                    Some(dt) => dt,
                    None => return TData::None,
                };
                let dt = scalar_return_type(builtin, &arg_datatypes);
                let dt = match dt {
                    Ok(dt) => dt,
                    Err(e) => {
                        info!(
                            "Could not get ouput datatype for {} and args {:?}: {}",
                            builtin, arg_datatypes, e
                        );
                        return TData::None;
                    }
                };
                TData::DataType(Self::check_datatype_cache(egraph, dt))
            }
            TokomakLogicalPlan::ScalarUDFCall(f) => {
                assert!(egraph[f.fun()].nodes.len() == 1);
                let udf = match &egraph[f.fun()].nodes[0] {
                    TokomakLogicalPlan::ScalarUDF(b) => b,
                    _ => return TData::None,
                };
                let arg_datatypes = Self::get_elist_datatypes(egraph, &f.args());
                let arg_datatypes = match arg_datatypes {
                    Some(dt) => dt,
                    None => return TData::None,
                };
                let sudf_impl = egraph.analysis.sudf_registry.get(&udf.0).unwrap();
                let dt = match (sudf_impl.return_type)(&arg_datatypes) {
                    Ok(dt) => dt,
                    Err(e) => {
                        panic!("Could not determine udf {} return type: {}", udf.0, e)
                    }
                };
                TData::DataType(dt)
            }
            TokomakLogicalPlan::AggregateBuiltinCall(f)
            | TokomakLogicalPlan::AggregateBuiltinDistinctCall(f) => {
                assert!(egraph[f.fun()].nodes.len() == 1);
                let agg_builtin = match &egraph[f.fun()].nodes[0] {
                    TokomakLogicalPlan::AggregateBuiltin(b) => b,
                    _ => return TData::None,
                };
                let arg_datatypes = Self::get_elist_datatypes(egraph, &f.args());
                let arg_datatypes = match arg_datatypes {
                    Some(dt) => dt,
                    None => return TData::None,
                };
                let dt = aggregate_return_type(agg_builtin, &arg_datatypes);
                let dt = match dt {
                    Ok(dt) => dt,
                    Err(e) => panic!(
                        "Could not determine agregate bultin {} return type: {}",
                        agg_builtin, e
                    ),
                };
                TData::DataType(RefCount::new(dt))
            }
            TokomakLogicalPlan::AggregateUDFCall(f) => {
                assert!(egraph[f.fun()].nodes.len() == 1);
                let udaf = match &egraph[f.fun()].nodes[0] {
                    TokomakLogicalPlan::AggregateUDF(b) => b,
                    _ => return TData::None,
                };
                let arg_datatypes = Self::get_elist_datatypes(egraph, &f.args());
                let arg_datatypes = match arg_datatypes {
                    Some(dt) => dt,
                    None => return TData::None,
                };
                let udaf_impl = egraph.analysis.udaf_registry.get(&udaf.0).unwrap();
                let dt = match (udaf_impl.return_type)(&arg_datatypes) {
                    Ok(dt) => dt,
                    Err(e) => {
                        panic!("Could not determine udaf {} return type: {}", udaf.0, e)
                    }
                };
                TData::DataType(dt)
            }
            TokomakLogicalPlan::WindowBuiltinCallUnframed(w) => {
                assert!(egraph[w.fun()].nodes.len() == 1);
                let window_fun = match &egraph[w.fun()].nodes[0] {
                    TokomakLogicalPlan::WindowBuiltin(w) => w,
                    p => panic!(
                        "WindowBuiltinCallUnframed expected WindowBuiltin found {}",
                        p
                    ),
                };
                let arg_datatypes = Self::get_elist_datatypes(egraph, &w.args_list());
                let arg_datatypes = match arg_datatypes {
                    Some(dt) => dt,
                    None => return TData::None,
                };
                let return_datatype = window_return_type(window_fun, &arg_datatypes);
                let dt = match return_datatype {
                    Ok(dt) => dt,
                    Err(e) => panic!(
                        "Could not determine window builtin {} return type: {}",
                        window_fun, e
                    ),
                };
                TData::DataType(RefCount::new(dt))
            }
            TokomakLogicalPlan::WindowBuiltinCallFramed(w) => {
                assert!(egraph[w.fun()].nodes.len() == 1);
                let window_fun = match &egraph[w.fun()].nodes[0] {
                    TokomakLogicalPlan::WindowBuiltin(w) => w,
                    p => panic!(
                        "WindowBuiltinCallUnframed expected WindowBuiltin found {}",
                        p
                    ),
                };
                let arg_datatypes = Self::get_elist_datatypes(egraph, &w.args_list());
                let arg_datatypes = match arg_datatypes {
                    Some(dt) => dt,
                    None => return TData::None,
                };
                let return_datatype = window_return_type(window_fun, &arg_datatypes);
                let dt = match return_datatype {
                    Ok(dt) => dt,
                    Err(e) => panic!(
                        "Could not determine window builtin {} return type: {}",
                        window_fun, e
                    ),
                };
                TData::DataType(RefCount::new(dt))
            }
            TokomakLogicalPlan::SortExpr(s) => get_data(&s.expr()),
            TokomakLogicalPlan::ExprAlias([input, ..]) => get_data(input),
            TokomakLogicalPlan::CaseIf(c) => Self::make_case_if(egraph, c),
            TokomakLogicalPlan::CaseLit(c) => Self::make_case_lit(egraph, c),
            //All columns should have data set when converting from datafusion expression so this should be okay.
            TokomakLogicalPlan::Column(_) => TData::None,
            TokomakLogicalPlan::TryCast([_expr, dt])
            | TokomakLogicalPlan::Cast([_expr, dt]) => get_data(dt),

            TokomakLogicalPlan::PList(_)
            | TokomakLogicalPlan::ScalarBuiltin(_)
            | TokomakLogicalPlan::AggregateBuiltin(_)
            | TokomakLogicalPlan::WindowBuiltin(_)
            | TokomakLogicalPlan::TableProject(_)
            | TokomakLogicalPlan::Table(_)
            | TokomakLogicalPlan::SortSpec(_)
            | TokomakLogicalPlan::ScalarUDF(_)
            | TokomakLogicalPlan::WindowFrame(_)
            | TokomakLogicalPlan::AggregateUDF(_)
            | TokomakLogicalPlan::Values(_)
            | TokomakLogicalPlan::Type(_)
            | TokomakLogicalPlan::JoinKeys(_)
            | TokomakLogicalPlan::Str(_)
            | TokomakLogicalPlan::None => TData::None,
        };
        data
    }
    //TODO: implement invariant check
    #[allow(dead_code)]
    fn test_invariants(
        _egraph: &EGraph<TokomakLogicalPlan, Self>,
        _enode: &TokomakLogicalPlan,
    ) {
        todo!()
    }
}

impl Analysis<TokomakLogicalPlan> for TokomakAnalysis {
    type Data = TData;
    #[cfg(feature = "invariant_verification")]
    fn make(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        enode: &TokomakLogicalPlan,
    ) -> Self::Data {
        Self::test_invariants(egraph, enode);
        Self::make_impl(egraph, enode)
    }
    #[cfg(not(feature = "invariant_verification"))]
    fn make(
        egraph: &EGraph<TokomakLogicalPlan, Self>,
        enode: &TokomakLogicalPlan,
    ) -> Self::Data {
        Self::make_impl(egraph, enode)
    }
    fn merge(&mut self, a: &mut Self::Data, b: Self::Data) -> DidMerge {
        if self.always_merge {
            *a = b;
            return DidMerge(true, false);
        }
        match (&(*a), b) {
            (TData::None, TData::DataType(dt)) => {
                *a = TData::DataType(dt);
                DidMerge(true, false)
            }
            (TData::None, TData::Schema(s)) => {
                *a = TData::Schema(s);
                DidMerge(true, false)
            }
            (_, TData::None) => DidMerge(false, false),
            (TData::DataType(adt), TData::DataType(bdt)) => {
                if *adt != bdt {
                    panic!("When merging found differing datatypes: [{},{}]", adt, bdt);
                }
                DidMerge(false, false)
            }
            (TData::DataType(dt), TData::Schema(schema)) => panic!(
                "Attempted to merge schema into datatype. dt={}  schema={}",
                dt, schema
            ),
            (TData::Schema(schema), TData::DataType(dt)) => panic!(
                "Attempted to merge datatype into schema.  schema={} dt={}",
                schema, dt
            ),
            (TData::Schema(aschema), TData::Schema(bschema)) => {
                if *aschema != bschema {
                    panic!(
                        "When merging found differing schemas: \n\ta='{}' \n\tb='{}'",
                        aschema, bschema
                    );
                }
                DidMerge(false, false)
            }
        }
    }
}

use std::fmt::Debug;

///The equality graph based optimizer
pub struct Tokomak {
    rules: Vec<Rewrite<TokomakLogicalPlan, TokomakAnalysis>>,
    added_builtins: BuiltinRulesFlag,
    runner_settings: RunnerSettings,
    name: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
///Flag that can be used to enable prebuilt rule sets
pub struct BuiltinRulesFlag(pub(crate) u32);
impl std::ops::BitOr for BuiltinRulesFlag {
    type Output = BuiltinRulesFlag;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitAnd for BuiltinRulesFlag {
    type Output = BuiltinRulesFlag;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}

impl std::ops::BitOrAssign for BuiltinRulesFlag {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

const ALL_BUILTIN_RULES: [BuiltinRulesFlag; 2] =
    [EXPR_SIMPLIFICATION_RULES, PLAN_SIMPLIFICATION_RULES];
///Flag to enable expression simplification rules
pub const EXPR_SIMPLIFICATION_RULES: BuiltinRulesFlag = BuiltinRulesFlag(0x1);
///Flag to enable plan simplification rules
pub const PLAN_SIMPLIFICATION_RULES: BuiltinRulesFlag = BuiltinRulesFlag(0x2);
const NO_RULES: BuiltinRulesFlag = BuiltinRulesFlag(0);
///Flag that will enable all builtin rules
pub const ALL_RULES: BuiltinRulesFlag = {
    let mut idx = 0;
    let mut flag = 0;
    while idx < ALL_BUILTIN_RULES.len() {
        flag |= ALL_BUILTIN_RULES[idx].0;
        idx += 1;
    }
    BuiltinRulesFlag(flag)
};

impl BuiltinRulesFlag {
    fn is_set(&self, other: BuiltinRulesFlag) -> bool {
        (*self & other) != NO_RULES
    }
}
impl Default for BuiltinRulesFlag {
    fn default() -> Self {
        NO_RULES
    }
}

#[derive(Default)]
///Allows setting limits on resource usage for the TokomakOptimzer on a per run basis.
pub struct RunnerSettings {
    ///The number of iterations that the optimizer will run. Defaults to 30.
    pub iter_limit: Option<usize>,
    ///The maximum allowed number of nodes in the egraph. Defaults to 10,000
    pub node_limit: Option<usize>,
    ///The maximum time that each full run is allowed to take. Note that this is checked at the end of an iteration so the optimizer can exceed this by the
    /// length of a full iteration. Defaults to 5 seconds.
    pub time_limit: Option<Duration>,
}
impl RunnerSettings {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
    fn create_runner(
        &self,
        egraph: EGraph<TokomakLogicalPlan, TokomakAnalysis>,
    ) -> Runner<TokomakLogicalPlan, TokomakAnalysis> {
        let mut runner = Runner::default().with_egraph(egraph);
        if let Some(iter_limit) = self.iter_limit {
            runner = runner.with_iter_limit(iter_limit);
        }
        if let Some(node_limit) = self.node_limit {
            runner = runner.with_node_limit(node_limit);
        }
        if let Some(time_limit) = self.time_limit {
            runner = runner.with_time_limit(time_limit);
        }
        runner
        //.with_scheduler(SimpleScheduler)
    }
    ///Sets the iteration limit
    pub fn with_iter_limit(&mut self, iter_limit: usize) -> &mut Self {
        self.iter_limit = Some(iter_limit);
        self
    }
    ///Sets the node limit
    pub fn with_node_limit(&mut self, node_limit: usize) -> &mut Self {
        self.node_limit = Some(node_limit);
        self
    }
    ///Sets the time limit
    pub fn with_time_limit(&mut self, time_limit: Duration) -> &mut Self {
        self.time_limit = Some(time_limit);
        self
    }

    fn optimize_plan<C: CostFunction<TokomakLogicalPlan>>(
        &self,
        root: Id,
        egraph: EGraph<TokomakLogicalPlan, TokomakAnalysis>,
        rules: &[Rewrite<TokomakLogicalPlan, TokomakAnalysis>],
        cost_func: C,
    ) -> Result<LogicalPlan, DataFusionError> {
        use std::time::Instant;
        let mut runner = self
            .create_runner(egraph)
            //.with_scheduler(SimpleScheduler);
            .with_scheduler(
                BackoffScheduler::default()
                    .with_initial_match_limit(2000)
                    //.rule_match_limit("expr-rotate-and", 40000)
                    //.rule_match_limit("expr-rotate-or", 40000)
                    //.rule_match_limit("expr-commute-and", 40000)
                    //.rule_match_limit("expr-commute-or", 40000)
                    .with_ban_length(5),
            );
        let start = std::time::Instant::now();

        runner = runner.run(rules.iter());
        let elapsed = start.elapsed();
        info!("Took {:.2}s optimizing the plan", elapsed.as_secs_f64());
        for (idx, iter) in runner.iterations.iter().enumerate() {
            info!("The iteration {} had {:#?}", idx, iter);
        }

        //let mut d = runner.egraph.dot();
        //d.use_anchors = false;
        //d.to_dot("/home/patrick/Documents/query.dot").unwrap();
        let ex = Extractor::new(&runner.egraph, cost_func);

        let start = Instant::now();
        let (cost, plan) = ex.find_best(root);
        let eclass_ids = runner.egraph.lookup_expr_ids(&plan).ok_or_else(|| {
            DataFusionError::Internal(String::from(
                "TokomakOptimizer could not extract plan from egraph.",
            ))
        })?;
        let elapsed = start.elapsed();
        info!("Took {:.2}s selecting the best plan", elapsed.as_secs_f64());
        info!("The lowest cost was {:?}", cost);
        let start = Instant::now();
        let plan = convert_to_df_plan(&plan, &eclass_ids, &runner.egraph).unwrap();
        let elapsed = start.elapsed();
        info!(
            "Took {:.2}s converting back to datafusion logical plan",
            elapsed.as_secs_f64()
        );
        Ok(plan)
    }
}

impl OptimizerRule for Tokomak {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _execution_props: &ExecutionProps,
    ) -> DFResult<LogicalPlan> {
        use std::time::Instant;
        let analysis = TokomakAnalysis::default();
        let mut egraph = EGraph::new(analysis);
        let start = Instant::now();
        let root = plan::to_tokomak_plan(plan, &mut egraph)?;
        let elapsed = start.elapsed();
        info!(
            "It took {:.2}s to convert to tokomak plan",
            elapsed.as_secs_f64()
        );
        egraph.analysis.always_merge = false;
        let optimized_plan = self.runner_settings.optimize_plan(
            root,
            egraph,
            &self.rules,
            DefaultCostFunc,
        )?;
        Ok(optimized_plan)
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }
}

impl Tokomak {
    ///Creates a TokomakOptimizer with a custom analysis
    pub fn new(runner_settings: RunnerSettings) -> Self {
        let name = "Tokomak".to_owned();
        Tokomak {
            added_builtins: NO_RULES,
            rules: Vec::new(),
            runner_settings,
            name,
        }
    }
    ///Creates a Tokomak optimizer with a custom analysis and the builtin rules defined by builtin_rules added.
    pub fn with_builtin_rules(
        runner_settings: RunnerSettings,
        builtin_rules: BuiltinRulesFlag,
    ) -> Self {
        let mut optimizer = Self::new(runner_settings);
        optimizer.add_builtin_rules(builtin_rules);
        optimizer
    }
    ///Adds the builtin rules defined by the builtin rules flag to the optimizer
    pub fn add_builtin_rules(&mut self, builtin_rules: BuiltinRulesFlag) {
        for rule in ALL_BUILTIN_RULES {
            //If the current flag is set and the ruleset has not been added to optimizer already
            if builtin_rules.is_set(rule) && !self.added_builtins.is_set(rule) {
                match rule {
                    EXPR_SIMPLIFICATION_RULES => self.add_expr_simplification_rules(),
                    PLAN_SIMPLIFICATION_RULES => self.add_plan_simplification_rules(),
                    _ => panic!("Found invalid rule flag"),
                }
            }
        }
    }
    ///Adds custom rules to the tokomak optmizer
    pub fn add_custom_rules(
        &mut self,
        custom_rules: Vec<Rewrite<TokomakLogicalPlan, TokomakAnalysis>>,
    ) {
        self.rules.extend(custom_rules.into_iter());
    }

    ///Adds builtin rules to the optimizer only if the filter Fn returns true.
    pub fn add_filtered_builtin_rules<
        F: Fn(&Rewrite<TokomakLogicalPlan, TokomakAnalysis>) -> bool,
    >(
        &mut self,
        builtin_rules: BuiltinRulesFlag,
        filter: F,
    ) {
        for rule in ALL_BUILTIN_RULES {
            //If the current flag is set and the ruleset has not been added to optimizer already
            if builtin_rules.is_set(rule) && !self.added_builtins.is_set(rule) {
                match rule {
                    EXPR_SIMPLIFICATION_RULES => {
                        self.add_filtered_expr_simplification_rules(&filter)
                    }
                    PLAN_SIMPLIFICATION_RULES => {
                        self.add_filtered_plan_simplification_rules(&filter)
                    }
                    _ => panic!("Found invalid rule flag"),
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct TokomakCost {
    plan_height: u32,
    cost: u64,
}
impl PartialEq for TokomakCost {
    fn eq(&self, other: &Self) -> bool {
        self.plan_height == other.plan_height && self.cost == other.cost
    }
}
impl PartialOrd for TokomakCost {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.cost.partial_cmp(&other.cost)
    }
}

struct DefaultCostFunc;
const PLAN_NODE_COST: u64 = 0x1 << 20;
impl CostFunction<TokomakLogicalPlan> for DefaultCostFunc {
    type Cost = TokomakCost;

    fn cost<C>(&mut self, enode: &TokomakLogicalPlan, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let (plan_height, plan_height_delta) = match enode {
            TokomakLogicalPlan::TableScan(_) | TokomakLogicalPlan::EmptyRelation(_) => {
                (0, 1)
            }
            p => {
                if p.is_expr() || p.is_supporting() {
                    (0, 0)
                } else {
                    (0, 1)
                }
            }
        };

        let mut cost = enode.fold(
            TokomakCost {
                cost: 0,
                plan_height,
            },
            |mut sum, id| {
                let child_cost = costs(id);
                sum.cost += child_cost.cost;
                sum.plan_height = sum
                    .plan_height
                    .max(child_cost.plan_height + plan_height_delta);
                sum
            },
        );

        let op_cost = match enode {
            TokomakLogicalPlan::Filter(_) => 0,
            // if cost.plan_height < 10{
            //    PLAN_NODE_COST - 20*(10 - cost.plan_height)
            //}else{
            //    PLAN_NODE_COST + 10*(cost.plan_height - 10)
            //},
            TokomakLogicalPlan::Projection(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::InnerJoin(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::LeftJoin(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::RightJoin(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::FullJoin(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::SemiJoin(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::AntiJoin(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::Extension(_) => PLAN_NODE_COST * PLAN_NODE_COST,
            TokomakLogicalPlan::CrossJoin(_) => {
                PLAN_NODE_COST * PLAN_NODE_COST * PLAN_NODE_COST
            }
            TokomakLogicalPlan::Limit(_) => 0,
            TokomakLogicalPlan::EmptyRelation(_) => 0,
            TokomakLogicalPlan::TableScan(_) => 0,
            //Union doesn't have any cost associated with it as its cost is sum of plans cost
            TokomakLogicalPlan::Union(_) => 0,
            TokomakLogicalPlan::Window(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::Aggregate(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::Sort(_) => PLAN_NODE_COST,
            TokomakLogicalPlan::PList(_) => 0,
            TokomakLogicalPlan::Hash(_) => 0,
            TokomakLogicalPlan::RoundRobinBatch(_) => 0,
            //These are supporting
            TokomakLogicalPlan::Table(_)
            | TokomakLogicalPlan::Values(_)
            | TokomakLogicalPlan::TableProject(_)
            | TokomakLogicalPlan::Str(_)
            | TokomakLogicalPlan::ExprAlias(_)
            | TokomakLogicalPlan::Type(_)
            | TokomakLogicalPlan::None => 0,
            TokomakLogicalPlan::JoinKeys(keys) => {
                assert!(keys.len() <= 512);
                512 - keys.len() as u64
            }
            //More items in list is almost always better
            TokomakLogicalPlan::EList(_) => 0,

            //Expressions use AstDepth for now
            TokomakLogicalPlan::Plus(_)
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
            | TokomakLogicalPlan::TryCast(_) => 1,
            TokomakLogicalPlan::CaseLit(c) => c.len() as u64,
            TokomakLogicalPlan::CaseIf(c) => c.len() as u64,
        };
        cost.cost += op_cost;
        cost
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
///Sort spec of SortExpr
pub enum SortSpec {
    ///Ascending
    Asc,
    ///Descending
    Desc,
    ///Ascending nulls first
    AscNullsFirst,
    ///Descending nulls first
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
///Name of UDF.
pub struct UDFName(pub Symbol);

impl Display for UDFName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl FromStr for UDFName {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let first_char = s.chars().next().ok_or_else(|| {
            DataFusionError::Internal("Zero length udf name".to_string())
        })?;
        if first_char == '?'
            || first_char.is_numeric()
            || first_char == '"'
            || first_char == '\''
        {
            return Err(DataFusionError::Internal(
                "Found ? or number as first char".to_string(),
            ));
        }
        Ok(UDFName(Symbol::from_str(s).unwrap()))
    }
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
///Name of Scalar UDF. Represented in the egraph is udf[<name>]. For the udf pow this would be udf[pow]
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
///Represented in the egraph as udaf[<name>]. For the udaf sum_times_2 this would be udaf[sum_times_2]
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
