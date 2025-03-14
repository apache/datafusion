use arrow::array::{Array, ArrayRef, AsArray, DynComparator, UnionArray};
use arrow::datatypes::{Float64Type, Int64Type};
use arrow_schema::{DataType, SortOptions};
use datafusion_common::cast::as_union_array;
use datafusion_common::types::{
    logical_float64, logical_int32, CustomOrdering, LogicalField, LogicalType,
    LogicalTypePlanningInformation, NativeType, SortOrdering, TypeSignature,
};
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

    fn default_cast_for(
        &self,
        _origin: &DataType,
    ) -> datafusion_common::Result<DataType> {
        unimplemented!()
    }

    fn planning_information(&self) -> LogicalTypePlanningInformation {
        LogicalTypePlanningInformation {
            ordering: SortOrdering::Custom(Arc::new(IntOrFloatTypeOrdering {})),
        }
    }
}

/// The order of the IntOrFloat is computed by converting both values to an `f64` and comparing
/// the resulting value.
#[derive(Debug)]
struct IntOrFloatTypeOrdering {}

impl CustomOrdering for IntOrFloatTypeOrdering {
    fn ordering_id(&self) -> &str {
        "order_int_or_float"
    }

    fn dyn_comparator(
        &self,
        array: ArrayRef,
        options: SortOptions,
    ) -> datafusion_common::Result<DynComparator> {
        // TODO check data type

        Ok(Box::new(move |lhs, rhs| {
            let array = as_union_array(array.as_ref()).expect("should be union");

            match (array.is_null(lhs), array.is_null(rhs)) {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if options.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (false, true) => {
                    if options.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (false, false) => {
                    let result = compare_impl(array, lhs, rhs);
                    match options.descending {
                        true => result.reverse(),
                        false => result,
                    }
                }
            }
        }))
    }
}

/// Default comparison between two (`lhs` & `rhs`) non-null [IntOrFloat] elements.
fn compare_impl(array: &UnionArray, lhs: usize, rhs: usize) -> Ordering {
    let type_lhs = array.type_ids()[lhs];
    let type_rhs = array.type_ids()[rhs];

    let offset_lhs = array.value_offset(lhs);
    let offset_rhs = array.value_offset(rhs);

    let lhs = match type_lhs {
        0 => {
            let array = array.child(type_lhs).as_primitive::<Int64Type>();
            array.value(offset_lhs) as f64
        }
        1 => {
            let array = array.child(type_lhs).as_primitive::<Float64Type>();
            array.value(offset_lhs)
        }
        _ => unreachable!("Union only has two variants"),
    };

    let rhs = match type_rhs {
        0 => {
            let array = array.child(type_rhs).as_primitive::<Int64Type>();
            array.value(offset_rhs) as f64
        }
        1 => {
            let array = array.child(type_rhs).as_primitive::<Float64Type>();
            array.value(offset_rhs)
        }
        _ => unreachable!("Union only has two variants"),
    };

    lhs.total_cmp(&rhs)
}
