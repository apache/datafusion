use arrow::array::{Array, ArrayRef, DynComparator, UInt32Array};
use arrow_schema::{DataType, SortOptions};
use datafusion_common::types::{
    logical_float64, logical_int32, CustomOrdering, LogicalField, LogicalType,
    LogicalTypePlanningInformation, NativeType, SortOrdering, TypeSignature,
};
use datafusion_common::ScalarValue;
use std::cmp::Ordering;
use std::sync::Arc;

/// Represents a type that is either an integer or a float.
pub struct IntOrFloatType {
    native_type: NativeType,
}

impl IntOrFloatType {
    pub fn name() -> &'static str {
        "int_or_float"
    }

    pub fn new() -> IntOrFloatType {
        let fields = [
            (
                0,
                Arc::new(LogicalField::new("integer", logical_int32(), false)),
            ),
            (
                1,
                Arc::new(LogicalField::new("float", logical_float64(), false)),
            ),
        ]
        .into_iter()
        .collect();
        Self {
            native_type: NativeType::Union(fields),
        }
    }
}

impl LogicalType for IntOrFloatType {
    fn native(&self) -> &NativeType {
        &self.native_type
    }

    fn signature(&self) -> TypeSignature<'_> {
        TypeSignature::Extension {
            name: Self::name(),
            parameters: &[],
        }
    }

    fn default_cast_for(&self, _origin: &DataType) -> datafusion_common::Result<DataType> {
        unimplemented!()
    }

    fn planning_information(&self) -> LogicalTypePlanningInformation {
        LogicalTypePlanningInformation {
            ordering: SortOrdering::Custom(Arc::new(IntOrFloatTypeOrdering {})),
        }
    }
}

/// The order of the IntOrFloat is defined as follows:
/// - All integers followed by all floats
/// - Within one subtype, the integers and floats are sorted using their natural order.
#[derive(Debug)]
struct IntOrFloatTypeOrdering {}

impl CustomOrdering for IntOrFloatTypeOrdering {
    fn ordering_id(&self) -> &str {
        "order_int_or_float"
    }

    fn compare_scalars(&self, _lhs: &ScalarValue, _rhs: &ScalarValue) -> Option<Ordering> {
        unimplemented!("TODO")
    }

    fn sort_to_indices(
        &self,
        _array: &dyn Array,
        _options: SortOptions,
        _fetch: Option<usize>,
    ) -> datafusion_common::Result<UInt32Array> {
        unimplemented!("TODO")
    }

    fn dyn_comparator(
        &self,
        _array: ArrayRef,
        _options: SortOptions,
    ) -> datafusion_common::Result<DynComparator> {
        unimplemented!("TODO")
    }
}
