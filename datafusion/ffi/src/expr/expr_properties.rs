// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_schema::SortOptions;
use datafusion_common::DataFusionError;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};

use crate::expr::interval::FFI_Interval;

/// A stable struct for sharing [`ExprProperties`] across FFI boundaries.
/// See [`ExprProperties`] for the meaning of each field.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ExprProperties {
    sort_properties: FFI_SortProperties,
    range: FFI_Interval,
    preserves_lex_ordering: bool,
    strictly_order_preserving: bool,
}

impl TryFrom<&ExprProperties> for FFI_ExprProperties {
    type Error = DataFusionError;
    fn try_from(value: &ExprProperties) -> Result<Self, Self::Error> {
        let sort_properties = (&value.sort_properties).into();
        let range = value.range.clone().try_into()?;

        Ok(FFI_ExprProperties {
            sort_properties,
            range,
            preserves_lex_ordering: value.preserves_lex_ordering,
            strictly_order_preserving: value.strictly_order_preserving,
        })
    }
}

impl TryFrom<FFI_ExprProperties> for ExprProperties {
    type Error = DataFusionError;
    fn try_from(value: FFI_ExprProperties) -> Result<Self, Self::Error> {
        let sort_properties = (&value.sort_properties).into();
        let range = value.range.try_into()?;
        Ok(ExprProperties {
            sort_properties,
            range,
            preserves_lex_ordering: value.preserves_lex_ordering,
            strictly_order_preserving: value.strictly_order_preserving,
        })
    }
}

#[repr(C)]
#[derive(Debug)]
pub enum FFI_SortProperties {
    Ordered(FFI_SortOptions),
    Unordered,
    Singleton,
}

impl From<&SortProperties> for FFI_SortProperties {
    fn from(value: &SortProperties) -> Self {
        match value {
            SortProperties::Unordered => FFI_SortProperties::Unordered,
            SortProperties::Singleton => FFI_SortProperties::Singleton,
            SortProperties::Ordered(o) => FFI_SortProperties::Ordered(o.into()),
        }
    }
}

impl From<&FFI_SortProperties> for SortProperties {
    fn from(value: &FFI_SortProperties) -> Self {
        match value {
            FFI_SortProperties::Unordered => SortProperties::Unordered,
            FFI_SortProperties::Singleton => SortProperties::Singleton,
            FFI_SortProperties::Ordered(o) => SortProperties::Ordered(o.into()),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct FFI_SortOptions {
    pub descending: bool,
    pub nulls_first: bool,
}

impl From<&SortOptions> for FFI_SortOptions {
    fn from(value: &SortOptions) -> Self {
        Self {
            descending: value.descending,
            nulls_first: value.nulls_first,
        }
    }
}

impl From<&FFI_SortOptions> for SortOptions {
    fn from(value: &FFI_SortOptions) -> Self {
        Self {
            descending: value.descending,
            nulls_first: value.nulls_first,
        }
    }
}
