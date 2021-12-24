use crate::{plan::TokomakLogicalPlan, Tokomak, TokomakAnalysis};
use datafusion::{arrow::datatypes::DataType, error::DataFusionError};
use egg::{
    Analysis, Applier, Condition, ConditionalApplier, EClass, EGraph, FromOp, Id,
    Language, LanguageChildren, RecExpr, RecExprParseError, Rewrite, SearchMatches,
    Searcher, Subst, Var,
};
use log::debug;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    marker::PhantomData,
    mem::{discriminant, Discriminant},
    str::FromStr,
};

pub fn transforming_pattern<A, TR>(
    name: &str,
    search: &str,
    applier: &str,
    transform: TR,
    bound_vars: &[Var],
) -> Result<Rewrite<TokomakLogicalPlan, A>, DataFusionError>
where
    A: Analysis<TokomakLogicalPlan> + 'static,
    TR: Send
        + Sync
        + 'static
        + Fn(&mut EGraph<TokomakLogicalPlan, A>, &mut Subst) -> Option<()>,
{
    let searcher = egg::Pattern::from_str(search).map_err(|e| {
        DataFusionError::Plan(format!(
            "Rule '{}' could not parse the searcher pattern : {}",
            name, e
        ))
    })?;
    let inner_applier: egg::Pattern<TokomakLogicalPlan> = egg::Pattern::from_str(applier)
        .map_err(|e| {
            DataFusionError::Plan(format!(
                "Rule '{}' could not parse the applier pattern : {}",
                name, e
            ))
        })?;
    let applier = ModifyingApplier::new(name, inner_applier, transform, bound_vars)?;
    let res =
        Rewrite::<TokomakLogicalPlan, A>::new(name, searcher, applier).map_err(|e| {
            DataFusionError::Plan(format!(
                "Rule '{}' could not create rewrite: {}",
                name, e
            ))
        });
    return res;
}

pub fn twoway_pattern<A: Analysis<TokomakLogicalPlan>>(
    name: &str,
    search: &str,
    applier: &str,
) -> Result<[Rewrite<TokomakLogicalPlan, A>; 2], DataFusionError> {
    let searcher = egg::Pattern::from_str(search).map_err(|e| {
        DataFusionError::Plan(format!(
            "Rule '{}' could not parse the searcher pattern : {}",
            name, e
        ))
    })?;
    let applier = egg::Pattern::from_str(applier).map_err(|e| {
        DataFusionError::Plan(format!(
            "Rule '{}' could not parse the applier pattern : {}",
            name, e
        ))
    })?;
    let mut name_buf = String::with_capacity(name.len() + 15);
    use std::fmt::Write;
    write!(name_buf, "{}_forwards", name).unwrap();
    let forwards = Rewrite::<TokomakLogicalPlan, A>::new(
        name_buf.as_str(),
        searcher.clone(),
        applier.clone(),
    )
    .map_err(|e| {
        DataFusionError::Plan(format!("Rule '{}' could not create rewrite: {}", name, e))
    })?;
    name_buf.clear();
    write!(name_buf, "{}_backwards", name).unwrap();
    let backwards =
        Rewrite::<TokomakLogicalPlan, A>::new(name_buf.as_str(), searcher, applier)
            .map_err(|e| {
                DataFusionError::Plan(format!(
                    "Rule '{}' could not create rewrite: {}",
                    name, e
                ))
            })?;
    Ok([forwards, backwards])
}

pub fn pattern<A: Analysis<TokomakLogicalPlan>>(
    name: &str,
    search: &str,
    applier: &str,
) -> Result<Rewrite<TokomakLogicalPlan, A>, DataFusionError> {
    let searcher = egg::Pattern::from_str(search).map_err(|e| {
        DataFusionError::Plan(format!(
            "Rule '{}' could not parse the searcher pattern : {}",
            name, e
        ))
    })?;
    let applier = egg::Pattern::from_str(applier).map_err(|e| {
        DataFusionError::Plan(format!(
            "Rule '{}' could not parse the applier pattern : {}",
            name, e
        ))
    })?;
    Rewrite::<TokomakLogicalPlan, A>::new(name, searcher, applier).map_err(|e| {
        DataFusionError::Plan(format!("Rule '{}' could not create rewrite: {}", name, e))
    })
}

pub fn conditional_rule<
    A: Analysis<TokomakLogicalPlan>,
    COND: Condition<TokomakLogicalPlan, A> + 'static + Send + Sync,
>(
    name: &str,
    search: &str,
    applier: &str,
    condition: COND,
) -> Result<Rewrite<TokomakLogicalPlan, A>, DataFusionError> {
    let searcher = egg::Pattern::from_str(search).map_err(|e| {
        DataFusionError::Plan(format!(
            "Rule '{}' could not parse the searcher pattern : {}",
            name, e
        ))
    })?;
    let applier = egg::Pattern::from_str(applier).map_err(|e| {
        DataFusionError::Plan(format!(
            "Rule '{}' could not parse the applier pattern : {}",
            name, e
        ))
    })?;
    let applier = ConditionalApplier { applier, condition };
    Rewrite::<TokomakLogicalPlan, A>::new(name, searcher, applier).map_err(|e| {
        DataFusionError::Plan(format!("Rule '{}' could not create rewrite: {}", name, e))
    })
}

pub struct ModifyingApplier<A, APP, TR>
where
    A: Analysis<TokomakLogicalPlan>,
    APP: Applier<TokomakLogicalPlan, A>,
    TR: Fn(&mut EGraph<TokomakLogicalPlan, A>, &mut Subst) -> Option<()>,
{
    inner_applier: APP,
    transform: TR,
    inner_bound_vars: Vec<Var>,
    _analysis: PhantomData<fn() -> A>,
}
impl<A, APP, TR> ModifyingApplier<A, APP, TR>
where
    A: Analysis<TokomakLogicalPlan>,
    APP: Applier<TokomakLogicalPlan, A>,
    TR: Fn(&mut EGraph<TokomakLogicalPlan, A>, &mut Subst) -> Option<()>,
{
    pub fn new(
        rule_name: &str,
        inner_applier: APP,
        transform: TR,
        bound_vars: &[Var],
    ) -> Result<Self, DataFusionError> {
        use std::collections::HashMap;
        use std::fmt::Write;
        let mut bound_set = bound_vars
            .iter()
            .map(|v| (v.clone(), false))
            .collect::<HashMap<Var, bool, fxhash::FxBuildHasher>>();
        let mut inner_bound_vars = inner_applier.vars();
        let mut unbound_vars = Vec::new();
        for i in (0..inner_bound_vars.len()).rev() {
            let var = inner_bound_vars[i];
            if let Some(bound) = bound_set.get_mut(&var) {
                *bound = true;
                inner_bound_vars.swap_remove(i);
            }
        }
        for (var, bound) in bound_set {
            if !bound {
                unbound_vars.push(var);
            }
        }
        if !unbound_vars.is_empty() {
            let mut err_msg = String::with_capacity(64 + unbound_vars.len() * 10);
            write!(err_msg, "ModifyingApplier for the rule {} found unbound vars. If these are generated by the rule add them to the bound_vars argument. The unbound vars found are: ", rule_name).unwrap();
            write!(err_msg, " {}", unbound_vars[0]).unwrap();
            for v in unbound_vars.iter().skip(1) {
                write!(err_msg, ", {}", v).unwrap();
            }
            return Err(DataFusionError::Plan(err_msg));
        }

        let applier = Self {
            inner_applier,
            transform,
            inner_bound_vars,
            _analysis: PhantomData,
        };
        Ok(applier)
    }

    fn apply_transform(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        subst: &mut Subst,
    ) -> Option<()> {
        (self.transform)(egraph, subst)
    }
}

impl<A, APP, TR> Applier<TokomakLogicalPlan, A> for ModifyingApplier<A, APP, TR>
where
    A: Analysis<TokomakLogicalPlan>,
    APP: Applier<TokomakLogicalPlan, A>,
    TR: Fn(&mut EGraph<TokomakLogicalPlan, A>, &mut Subst) -> Option<()>,
{
    fn apply_one(
        &self,
        egraph: &mut EGraph<TokomakLogicalPlan, A>,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&egg::PatternAst<TokomakLogicalPlan>>,
        rule_name: egg::Symbol,
    ) -> Vec<Id> {
        let mut new_subst = subst.clone();
        match self.apply_transform(egraph, &mut new_subst) {
            Some(_) => self.inner_applier.apply_one(
                egraph,
                eclass,
                &new_subst,
                searcher_ast,
                rule_name,
            ),
            None => vec![],
        }
    }
    //This applier slightly lies about what variables it binds, since it creates one of the bound vars for the inner applier
    fn vars(&self) -> Vec<Var> {
        self.inner_bound_vars.clone()
    }
}
