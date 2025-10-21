use crate::expr::interval::FFI_Interval;
use abi_stable::StableAbi;
use arrow_schema::SortOptions;
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_common::DataFusionError;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ExprProperties {
    pub sort_properties: FFI_SortProperties,
    pub range: FFI_Interval,
    pub preserves_lex_ordering: bool,
}

impl TryFrom<&ExprProperties> for FFI_ExprProperties {
    type Error = DataFusionError;
    fn try_from(value: &ExprProperties) -> Result<Self, Self::Error> {
        let sort_properties = (&value.sort_properties).into();
        let range = (&value.range).try_into()?;

        Ok(FFI_ExprProperties {
            sort_properties,
            range,
            preserves_lex_ordering: value.preserves_lex_ordering,
        })
    }
}

impl TryFrom<&FFI_ExprProperties> for ExprProperties {
    type Error = DataFusionError;
    fn try_from(value: &FFI_ExprProperties) -> Result<Self, Self::Error> {
        let sort_properties = (&value.sort_properties).into();
        let range = (&value.range).try_into()?;
        Ok(ExprProperties {
            sort_properties,
            range,
            preserves_lex_ordering: value.preserves_lex_ordering,
        })
    }
}

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
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
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
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
