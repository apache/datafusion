use std::num::NonZeroUsize;

use abi_stable::{std_types::RVec, StableAbi};
use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::DataType;
use datafusion::{
    error::DataFusionError,
    logical_expr::{
        ArrayFunctionSignature, Signature, TypeSignature, TypeSignatureClass, Volatility,
    },
};

use crate::arrow_wrappers::WrappedSchema;
use datafusion::error::Result;

#[repr(C)]
#[derive(StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_Signature {
    pub type_signature: FFI_TypeSignature,
    pub volatility: FFI_Volatility,
}

impl TryFrom<&Signature> for FFI_Signature {
    type Error = DataFusionError;

    fn try_from(value: &Signature) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            type_signature: (&value.type_signature).try_into()?,
            volatility: (&value.volatility).into(),
        })
    }
}

impl TryFrom<&FFI_Signature> for Signature {
    type Error = DataFusionError;

    fn try_from(value: &FFI_Signature) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            type_signature: (&value.type_signature).try_into()?,
            volatility: (&value.volatility).into(),
        })
    }
}

#[repr(C)]
#[derive(StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_Volatility {
    Immutable,
    Stable,
    Volatile,
}

impl From<&Volatility> for FFI_Volatility {
    fn from(value: &Volatility) -> Self {
        match value {
            Volatility::Immutable => Self::Immutable,
            Volatility::Stable => Self::Stable,
            Volatility::Volatile => Self::Volatile,
        }
    }
}

impl From<&FFI_Volatility> for Volatility {
    fn from(value: &FFI_Volatility) -> Self {
        match value {
            FFI_Volatility::Immutable => Self::Immutable,
            FFI_Volatility::Stable => Self::Stable,
            FFI_Volatility::Volatile => Self::Volatile,
        }
    }
}

#[repr(C)]
#[derive(StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_TypeSignatureClass {
    Timestamp,
    Date,
    Time,
    Interval,
    Duration,
    // TODO implement Native
}

impl TryFrom<&TypeSignatureClass> for FFI_TypeSignatureClass {
    type Error = DataFusionError;

    fn try_from(value: &TypeSignatureClass) -> std::result::Result<Self, Self::Error> {
        match value {
            TypeSignatureClass::Timestamp => Ok(Self::Timestamp),
            TypeSignatureClass::Date => Ok(Self::Date),
            TypeSignatureClass::Time => Ok(Self::Time),
            TypeSignatureClass::Interval => Ok(Self::Interval),
            TypeSignatureClass::Duration => Ok(Self::Duration),
            TypeSignatureClass::Native(_) => Err(DataFusionError::NotImplemented(
                "Native TypeSignatureClass is not supported via FFI".to_string(),
            )),
        }
    }
}

impl From<&FFI_TypeSignatureClass> for TypeSignatureClass {
    fn from(value: &FFI_TypeSignatureClass) -> Self {
        match value {
            FFI_TypeSignatureClass::Timestamp => Self::Timestamp,
            FFI_TypeSignatureClass::Date => Self::Date,
            FFI_TypeSignatureClass::Time => Self::Time,
            FFI_TypeSignatureClass::Interval => Self::Interval,
            FFI_TypeSignatureClass::Duration => Self::Duration,
        }
    }
}

#[repr(C)]
#[derive(StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_TypeSignature {
    Variadic(RVec<WrappedSchema>),
    UserDefined,
    VariadicAny,
    Uniform(usize, RVec<WrappedSchema>),
    Exact(RVec<WrappedSchema>),
    Coercible(RVec<FFI_TypeSignatureClass>),
    Comparable(usize),
    Any(usize),
    OneOf(RVec<FFI_TypeSignature>),
    ArraySignature(FFI_ArrayFunctionSignature),
    Numeric(usize),
    String(usize),
    Nullary,
}

pub fn vec_datatype_to_rvec_wrapped(
    data_types: &[DataType],
) -> std::result::Result<RVec<WrappedSchema>, arrow_schema::ArrowError> {
    Ok(data_types
        .iter()
        .map(FFI_ArrowSchema::try_from)
        .collect::<Result<Vec<_>, arrow_schema::ArrowError>>()?
        .into_iter()
        .map(WrappedSchema)
        .collect())
}

pub fn rvec_wrapped_to_vec_datatype(
    data_types: &RVec<WrappedSchema>,
) -> std::result::Result<Vec<DataType>, arrow_schema::ArrowError> {
    data_types
        .iter()
        .map(|d| DataType::try_from(&d.0))
        .collect()
}

impl TryFrom<&TypeSignature> for FFI_TypeSignature {
    type Error = DataFusionError;

    fn try_from(value: &TypeSignature) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            TypeSignature::Variadic(data_types) => {
                Self::Variadic(vec_datatype_to_rvec_wrapped(data_types)?)
            }
            TypeSignature::UserDefined => Self::UserDefined,
            TypeSignature::VariadicAny => Self::VariadicAny,
            TypeSignature::Uniform(num, data_types) => {
                Self::Uniform(num.to_owned(), vec_datatype_to_rvec_wrapped(data_types)?)
            }
            TypeSignature::Exact(data_types) => {
                Self::Exact(vec_datatype_to_rvec_wrapped(data_types)?)
            }
            TypeSignature::Coercible(items) => Self::Coercible(
                items
                    .iter()
                    .map(FFI_TypeSignatureClass::try_from)
                    .collect::<Result<Vec<_>>>()?
                    .into(),
            ),
            TypeSignature::Comparable(num) => Self::Comparable(num.to_owned()),
            TypeSignature::Any(num) => Self::Any(num.to_owned()),
            TypeSignature::OneOf(type_signatures) => Self::OneOf(
                type_signatures
                    .iter()
                    .map(FFI_TypeSignature::try_from)
                    .collect::<Result<Vec<_>>>()?
                    .into(),
            ),
            TypeSignature::ArraySignature(s) => Self::ArraySignature(s.into()),
            TypeSignature::Numeric(num) => Self::Numeric(num.to_owned()),
            TypeSignature::String(num) => Self::String(num.to_owned()),
            TypeSignature::Nullary => Self::Nullary,
        })
    }
}

impl TryFrom<&FFI_TypeSignature> for TypeSignature {
    type Error = DataFusionError;

    fn try_from(value: &FFI_TypeSignature) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            FFI_TypeSignature::Variadic(data_types) => {
                Self::Variadic(rvec_wrapped_to_vec_datatype(data_types)?)
            }
            FFI_TypeSignature::UserDefined => Self::UserDefined,
            FFI_TypeSignature::VariadicAny => Self::VariadicAny,
            FFI_TypeSignature::Uniform(num, data_types) => {
                Self::Uniform(num.to_owned(), rvec_wrapped_to_vec_datatype(data_types)?)
            }
            FFI_TypeSignature::Exact(data_types) => {
                Self::Exact(rvec_wrapped_to_vec_datatype(data_types)?)
            }
            FFI_TypeSignature::Coercible(items) => {
                Self::Coercible(items.iter().map(|c| c.into()).collect())
            }
            FFI_TypeSignature::Comparable(num) => Self::Comparable(num.to_owned()),
            FFI_TypeSignature::Any(num) => Self::Any(num.to_owned()),
            FFI_TypeSignature::OneOf(type_signatures) => Self::OneOf({
                type_signatures
                    .iter()
                    .map(TypeSignature::try_from)
                    .collect::<Result<Vec<_>>>()?
            }),
            FFI_TypeSignature::ArraySignature(s) => Self::ArraySignature(s.into()),
            FFI_TypeSignature::Numeric(num) => Self::Numeric(num.to_owned()),
            FFI_TypeSignature::String(num) => Self::String(num.to_owned()),
            FFI_TypeSignature::Nullary => Self::Nullary,
        })
    }
}

#[repr(C)]
#[derive(StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_ArrayFunctionSignature {
    ArrayAndElement,
    ElementAndArray,
    ArrayAndIndexes(NonZeroUsize),
    ArrayAndElementAndOptionalIndex,
    Array,
    RecursiveArray,
    MapArray,
}

impl From<&ArrayFunctionSignature> for FFI_ArrayFunctionSignature {
    fn from(value: &ArrayFunctionSignature) -> Self {
        match value {
            ArrayFunctionSignature::ArrayAndElement => Self::ArrayAndElement,
            ArrayFunctionSignature::ElementAndArray => Self::ElementAndArray,
            ArrayFunctionSignature::ArrayAndIndexes(non_zero) => {
                Self::ArrayAndIndexes(non_zero.to_owned())
            }
            ArrayFunctionSignature::ArrayAndElementAndOptionalIndex => {
                Self::ArrayAndElementAndOptionalIndex
            }
            ArrayFunctionSignature::Array => Self::Array,
            ArrayFunctionSignature::RecursiveArray => Self::RecursiveArray,
            ArrayFunctionSignature::MapArray => Self::MapArray,
        }
    }
}

impl From<&FFI_ArrayFunctionSignature> for ArrayFunctionSignature {
    fn from(value: &FFI_ArrayFunctionSignature) -> Self {
        match value {
            FFI_ArrayFunctionSignature::ArrayAndElement => Self::ArrayAndElement,
            FFI_ArrayFunctionSignature::ElementAndArray => Self::ElementAndArray,
            FFI_ArrayFunctionSignature::ArrayAndIndexes(non_zero) => {
                Self::ArrayAndIndexes(non_zero.to_owned())
            }
            FFI_ArrayFunctionSignature::ArrayAndElementAndOptionalIndex => {
                Self::ArrayAndElementAndOptionalIndex
            }
            FFI_ArrayFunctionSignature::Array => Self::Array,
            FFI_ArrayFunctionSignature::RecursiveArray => Self::RecursiveArray,
            FFI_ArrayFunctionSignature::MapArray => Self::MapArray,
        }
    }
}
