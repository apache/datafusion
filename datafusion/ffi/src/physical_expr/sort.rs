// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_schema::SortOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{
    LexRequirement, OrderingRequirements, PhysicalSortRequirement,
};
use stabby::vec::Vec as SVec;

use crate::expr::expr_properties::FFI_SortOptions;
use crate::physical_expr::FFI_PhysicalExpr;
use crate::util::FFI_Option;

/// A stable struct for sharing [`PhysicalSortExpr`] across FFI boundaries.
/// See [`PhysicalSortExpr`] for the meaning of each field.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_PhysicalSortExpr {
    expr: FFI_PhysicalExpr,
    options: FFI_SortOptions,
}

impl From<&PhysicalSortExpr> for FFI_PhysicalSortExpr {
    fn from(value: &PhysicalSortExpr) -> Self {
        let expr = FFI_PhysicalExpr::from(value.clone().expr);
        let options = FFI_SortOptions::from(&value.options);

        Self { expr, options }
    }
}

impl From<&FFI_PhysicalSortExpr> for PhysicalSortExpr {
    fn from(value: &FFI_PhysicalSortExpr) -> Self {
        let expr: Arc<dyn PhysicalExpr> = (&value.expr).into();
        let options = SortOptions::from(&value.options);

        Self { expr, options }
    }
}

/// FFI representation of a [`PhysicalSortRequirement`].
#[repr(C)]
#[derive(Debug)]
pub struct FFI_PhysicalSortRequirement {
    expr: FFI_PhysicalExpr,
    options: FFI_Option<FFI_SortOptions>,
}

impl From<&PhysicalSortRequirement> for FFI_PhysicalSortRequirement {
    fn from(value: &PhysicalSortRequirement) -> Self {
        Self {
            expr: FFI_PhysicalExpr::from(Arc::clone(&value.expr)),
            options: value.options.as_ref().map(FFI_SortOptions::from).into(),
        }
    }
}

impl From<&FFI_PhysicalSortRequirement> for PhysicalSortRequirement {
    fn from(value: &FFI_PhysicalSortRequirement) -> Self {
        let expr: Arc<dyn PhysicalExpr> = (&value.expr).into();
        let options = match &value.options {
            FFI_Option::Some(options) => Some(SortOptions::from(options)),
            FFI_Option::None => None,
        };
        Self { expr, options }
    }
}

/// FFI representation of [`OrderingRequirements`]. Each inner vector is one
/// alternative [`LexRequirement`].
#[repr(C)]
#[derive(Debug)]
pub enum FFI_OrderingRequirements {
    Hard(SVec<SVec<FFI_PhysicalSortRequirement>>),
    Soft(SVec<SVec<FFI_PhysicalSortRequirement>>),
}

fn alternatives_to_ffi(
    alternatives: &[LexRequirement],
) -> SVec<SVec<FFI_PhysicalSortRequirement>> {
    alternatives
        .iter()
        .map(|lex| lex.iter().map(FFI_PhysicalSortRequirement::from).collect())
        .collect()
}

fn alternatives_from_ffi(
    alternatives: &SVec<SVec<FFI_PhysicalSortRequirement>>,
) -> Result<Vec<LexRequirement>> {
    alternatives
        .iter()
        .map(|lex| {
            LexRequirement::new(lex.iter().map(PhysicalSortRequirement::from)).ok_or_else(
                || {
                    DataFusionError::Internal(
                        "FFI ordering requirement must be non-empty".to_string(),
                    )
                },
            )
        })
        .collect()
}

impl From<&OrderingRequirements> for FFI_OrderingRequirements {
    fn from(value: &OrderingRequirements) -> Self {
        match value {
            OrderingRequirements::Hard(alts) => Self::Hard(alternatives_to_ffi(alts)),
            OrderingRequirements::Soft(alts) => Self::Soft(alternatives_to_ffi(alts)),
        }
    }
}

impl TryFrom<&FFI_OrderingRequirements> for OrderingRequirements {
    type Error = DataFusionError;

    fn try_from(value: &FFI_OrderingRequirements) -> Result<Self> {
        let (alternatives, soft) = match value {
            FFI_OrderingRequirements::Hard(alts) => (alternatives_from_ffi(alts)?, false),
            FFI_OrderingRequirements::Soft(alts) => (alternatives_from_ffi(alts)?, true),
        };
        OrderingRequirements::new_alternatives(alternatives, soft).ok_or_else(|| {
            DataFusionError::Internal(
                "FFI ordering requirements must have at least one alternative"
                    .to_string(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::SortOptions;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

    use crate::physical_expr::sort::FFI_PhysicalSortExpr;

    #[test]
    fn ffi_sort_expr_round_trip() {
        let col_expr = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let expr = PhysicalSortExpr::new(col_expr, SortOptions::default());

        let ffi_expr = FFI_PhysicalSortExpr::from(&expr);
        let foreign_expr = PhysicalSortExpr::from(&ffi_expr);

        assert_eq!(expr, foreign_expr);
    }

    #[test]
    fn ffi_ordering_requirements_round_trip() -> datafusion_common::Result<()> {
        use datafusion_physical_expr_common::sort_expr::{
            LexRequirement, OrderingRequirements, PhysicalSortRequirement,
        };

        use crate::physical_expr::sort::FFI_OrderingRequirements;

        let a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let b = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;
        let first = LexRequirement::new([
            PhysicalSortRequirement::new(
                Arc::clone(&a),
                Some(SortOptions::new(true, false)),
            ),
            PhysicalSortRequirement::new(Arc::clone(&b), None),
        ])
        .expect("non-empty");
        let second = LexRequirement::new([PhysicalSortRequirement::new(b, None)])
            .expect("non-empty");

        for soft in [false, true] {
            let original = OrderingRequirements::new_alternatives(
                [first.clone(), second.clone()],
                soft,
            )
            .expect("non-empty");
            let ffi = FFI_OrderingRequirements::from(&original);
            let returned = OrderingRequirements::try_from(&ffi)?;
            assert_eq!(original, returned);
        }
        Ok(())
    }
}
