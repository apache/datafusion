mod boolean_lookup_table;
mod bytes_like_lookup_table;
mod primitive_lookup_table;

use std::fmt::Debug;
use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;

/// Lookup table for mapping literal values to their corresponding indices
///
/// The else index is used when a value is not found in the lookup table
pub(super) trait LookupTable: Debug + Send + Sync {
  /// Try creating a new lookup table from the given literals and else index
  fn try_new(literals: Vec<ScalarValue>, else_index: i32) -> datafusion_common::Result<Self>
  where
    Self: Sized;

  /// Return indices to take from the literals based on the values in the given array
  fn match_values(&self, array: &ArrayRef) -> datafusion_common::Result<Vec<i32>>;
}

pub(crate) fn try_creating_lookup_table(
  literals: Vec<ScalarValue>,
  else_index: i32,
) -> datafusion_common::Result<Arc<dyn LookupTable>> {
  assert_ne!(literals.len(), 0, "Must have at least one literal");
  match literals[0].data_type() {
    DataType::Boolean => {
      let lookup_table = BooleanLookupMap::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    data_type if data_type.is_primitive() => {
      macro_rules! create_matching_map {
                ($t:ty) => {{
                    let lookup_table =
                        PrimitiveArrayMapHolder::<$t>::try_new(literals, else_index)?;
                    Ok(Arc::new(lookup_table))
                }};
            }

      downcast_primitive! {
                data_type => (create_matching_map),
                _ => Err(plan_datafusion_err!(
                    "Unsupported field type for primitive: {:?}",
                    data_type
                )),
            }
    }

    DataType::Utf8 => {
      let lookup_table =
        BytesLookupTable::<GenericBytesHelper<GenericStringType<i32>>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::LargeUtf8 => {
      let lookup_table =
        BytesLookupTable::<GenericBytesHelper<GenericStringType<i64>>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::Binary => {
      let lookup_table =
        BytesLookupTable::<GenericBytesHelper<GenericBinaryType<i32>>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::LargeBinary => {
      let lookup_table =
        BytesLookupTable::<GenericBytesHelper<GenericBinaryType<i64>>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::FixedSizeBinary(_) => {
      let lookup_table =
        BytesLookupTable::<FixedBinaryHelper>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::Utf8View => {
      let lookup_table =
        BytesLookupTable::<GenericBytesViewHelper<StringViewType>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }
    DataType::BinaryView => {
      let lookup_table =
        BytesLookupTable::<GenericBytesViewHelper<BinaryViewType>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::Dictionary(key, value) => {
      macro_rules! downcast_dictionary_array_helper {
                ($t:ty) => {{
                    create_lookup_table_for_dictionary_input::<$t>(
                        value.as_ref(),
                        literals,
                        else_index,
                    )
                }};
            }

      downcast_integer! {
                key.as_ref() => (downcast_dictionary_array_helper),
                k => unreachable!("unsupported dictionary key type: {}", k)
            }
    }
    _ => Err(plan_datafusion_err!(
            "Unsupported data type for lookup table: {}",
            literals[0].data_type()
        )),
  }
}

fn create_lookup_table_for_dictionary_input<K: ArrowDictionaryKeyType + Send + Sync>(
  value: &DataType,
  literals: Vec<ScalarValue>,
  else_index: i32,
) -> datafusion_common::Result<Arc<dyn LookupTable>> {
  match value {
    DataType::Utf8 => {
      let lookup_table = BytesLookupTable::<BytesDictionaryHelper::<K, GenericStringType<i32>>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::LargeUtf8 => {
      let lookup_table = BytesLookupTable::<BytesDictionaryHelper::<K, GenericStringType<i64>>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::Binary => {
      let lookup_table = BytesLookupTable::<BytesDictionaryHelper::<K, GenericBinaryType<i32>>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::LargeBinary => {
      let lookup_table = BytesLookupTable::<BytesDictionaryHelper::<K, GenericBinaryType<i64>>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::FixedSizeBinary(_) => {
      let lookup_table =BytesLookupTable::<FixedBytesDictionaryHelper<K>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }

    DataType::Utf8View => {
      let lookup_table = BytesLookupTable::<BytesViewDictionaryHelper::<K, StringViewType>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }
    DataType::BinaryView => {
      let lookup_table = BytesLookupTable::<BytesViewDictionaryHelper::<K, BinaryViewType>>::try_new(literals, else_index)?;
      Ok(Arc::new(lookup_table))
    }
    _ => Err(plan_datafusion_err!(
            "Unsupported dictionary value type for lookup table: {}",
            value
        )),
  }
}