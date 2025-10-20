use crate::expressions::Literal;
use arrow::array::AsArray;
use arrow::array::{downcast_integer, downcast_primitive, Array, ArrayAccessor, ArrayIter, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, FixedSizeBinaryArray, FixedSizeBinaryIter, GenericByteViewArray, TypedDictionaryArray};
use arrow::array::GenericByteArray;
use arrow::datatypes::{i256, ArrowDictionaryKeyType, BinaryViewType, ByteArrayType, ByteViewType, DataType, GenericBinaryType, GenericStringType, IntervalDayTime, IntervalMonthDayNano, StringViewType};
use datafusion_common::{exec_datafusion_err, plan_datafusion_err, ScalarValue};
use half::f16;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash};
use std::iter::Map;
use std::marker::PhantomData;
use std::sync::Arc;

/// Lookup table for mapping literal values to their corresponding indices
///
/// The else index is used when a value is not found in the lookup table
pub(crate) trait LookupTable: Debug + Send + Sync {
  /// Try creating a new lookup table from the given literals and else index
  fn try_new(literals: &[Arc<Literal>], else_index: i32) -> datafusion_common::Result<Self>
  where
    Self: Sized;

  /// Return indices to take from the literals based on the values in the given array
  fn match_values(&self, array: &ArrayRef) -> datafusion_common::Result<Vec<i32>>;
}

pub(crate) fn try_creating_lookup_table(
  literals: &[Arc<Literal>],
  else_index: i32,
) -> datafusion_common::Result<Arc<dyn LookupTable>> {
  assert_ne!(literals.len(), 0, "Must have at least one literal");
  match literals[0].value().data_type() {
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
            literals[0].value().data_type()
        )),
  }
}

fn create_lookup_table_for_dictionary_input<K: ArrowDictionaryKeyType + Send + Sync>(
  value: &DataType,
  literals: &[Arc<Literal>],
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

#[derive(Clone)]
struct PrimitiveArrayMapHolder<T>
where
  T: ArrowPrimitiveType,
  T::Native: ToHashableKey,
{
  /// Literal value to map index
  ///
  /// If searching this map becomes a bottleneck consider using linear map implementations for small hashmaps
  map: HashMap<Option<<T::Native as ToHashableKey>::HashableKey>, i32>,
  else_index: i32,
}

impl<T> LookupTable for PrimitiveArrayMapHolder<T>
where
  T: ArrowPrimitiveType,
  T::Native: ToHashableKey,
{
  fn try_new(literals: &[Arc<Literal>], else_index: i32) -> datafusion_common::Result<Self>
  where
    Self: Sized,
  {
    let input = ScalarValue::iter_to_array(literals.iter().map(|item| item.value().clone()))?;

    let map = input
      .as_primitive::<T>()
      .into_iter()
      .enumerate()
      .map(|(map_index, value)| (value.map(|v| v.into_hashable_key()), map_index as i32))
      .collect();

    Ok(Self { map, else_index })
  }

  fn match_values(&self, array: &ArrayRef) -> datafusion_common::Result<Vec<i32>> {
    let indices = array
      .as_primitive::<T>()
      .into_iter()
      .map(|value| self.map.get(&value.map(|item| item.into_hashable_key())).copied().unwrap_or(self.else_index))
      .collect::<Vec<i32>>();

    Ok(indices)
  }
}


trait BytesMapHelperWrapperTrait: Send + Sync
{
  type IntoIter<'a>: Iterator<Item=Option<&'a [u8]>> + 'a;
  fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>>;
}


#[derive(Debug, Clone, Default)]
struct GenericBytesHelper<T: ByteArrayType>(PhantomData<T>);

impl<T: ByteArrayType> BytesMapHelperWrapperTrait for GenericBytesHelper<T> {
  type IntoIter<'a> = Map<ArrayIter<&'a GenericByteArray<T>>, fn(Option<&'a <T as ByteArrayType>::Native>) -> Option<&[u8]>>;

  fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
    Ok(array
      .as_bytes::<T>()
      .into_iter()
      .map(|item| {
      item.map(|v| {
        let bytes: &[u8] = v.as_ref();

        bytes
      })
    }))
  }
}

#[derive(Debug, Clone, Default)]
struct FixedBinaryHelper;

impl BytesMapHelperWrapperTrait for FixedBinaryHelper {
  type IntoIter<'a> = FixedSizeBinaryIter<'a>;

  fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
    Ok(array.as_fixed_size_binary().into_iter())
  }
}


#[derive(Debug, Clone, Default)]
struct GenericBytesViewHelper<T: ByteViewType>(PhantomData<T>);
impl<T: ByteViewType> BytesMapHelperWrapperTrait for GenericBytesViewHelper<T> {
  type IntoIter<'a> = Map<ArrayIter<&'a GenericByteViewArray<T>>, fn(Option<&'a <T as ByteViewType>::Native>) -> Option<&[u8]>>;

  fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
    Ok(array.as_byte_view::<T>().into_iter().map(|item| {
      item.map(|v| {
        let bytes: &[u8] = v.as_ref();

        bytes
      })
    }))
  }
}


#[derive(Debug, Clone, Default)]
struct BytesDictionaryHelper<Key: ArrowDictionaryKeyType, Value: ByteArrayType>(PhantomData<(Key, Value)>);

impl<Key, Value> BytesMapHelperWrapperTrait for BytesDictionaryHelper<Key, Value>
where
  Key: ArrowDictionaryKeyType + Send + Sync,
  Value: ByteArrayType,
  for<'a> TypedDictionaryArray<'a, Key, GenericByteArray<Value>>:
  IntoIterator<Item=Option<&'a Value::Native>> {
  type IntoIter<'a> = Map<<TypedDictionaryArray<'a, Key, GenericByteArray<Value>> as IntoIterator>::IntoIter, fn(Option<&'a <Value as ByteArrayType>::Native>) -> Option<&[u8]>>;

  fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
    let dict_array = array
      .as_dictionary::<Key>()
      .downcast_dict::<GenericByteArray<Value>>()
      .ok_or_else(|| {
        exec_datafusion_err!(
          "Failed to downcast dictionary array {} to expected dictionary value {}",
          array.data_type(),
          Value::DATA_TYPE
        )
      })?;

    Ok(dict_array.into_iter().map(|item| item.map(|v| {
      let bytes: &[u8] = v.as_ref();

      bytes
    })))
  }
}

#[derive(Debug, Clone, Default)]
struct FixedBytesDictionaryHelper<Key: ArrowDictionaryKeyType>(PhantomData<Key>);

impl<Key> BytesMapHelperWrapperTrait for FixedBytesDictionaryHelper<Key>
where
  Key: ArrowDictionaryKeyType + Send + Sync,
  for<'a> TypedDictionaryArray<'a, Key, FixedSizeBinaryArray>: IntoIterator<Item=Option<&'a [u8]>> {
  type IntoIter<'a> = <TypedDictionaryArray<'a, Key, FixedSizeBinaryArray> as IntoIterator>::IntoIter;

  fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
    let dict_array = array
      .as_dictionary::<Key>()
      .downcast_dict::<FixedSizeBinaryArray>()
      .ok_or_else(|| {
        exec_datafusion_err!(
          "Failed to downcast dictionary array {} to expected dictionary fixed size binary values",
          array.data_type()
        )
      })?;

    Ok(dict_array.into_iter())
  }
}

#[derive(Debug, Clone, Default)]
struct BytesViewDictionaryHelper<Key: ArrowDictionaryKeyType, Value: ByteViewType>(PhantomData<(Key, Value)>);

impl<Key, Value> BytesMapHelperWrapperTrait for BytesViewDictionaryHelper<Key, Value>
where
  Key: ArrowDictionaryKeyType + Send + Sync,
  Value: ByteViewType,
  for<'a> TypedDictionaryArray<'a, Key, GenericByteViewArray<Value>>:
  IntoIterator<Item=Option<&'a Value::Native>> {
  type IntoIter<'a> = Map<<TypedDictionaryArray<'a, Key, GenericByteViewArray<Value>> as IntoIterator>::IntoIter, fn(Option<&'a <Value as ByteViewType>::Native>) -> Option<&[u8]>>;

  fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
    let dict_array = array
      .as_dictionary::<Key>()
      .downcast_dict::<GenericByteViewArray<Value>>()
      .ok_or_else(|| {
        exec_datafusion_err!(
          "Failed to downcast dictionary array {} to expected dictionary value {}",
          array.data_type(),
          Value::DATA_TYPE
        )
      })?;

    Ok(dict_array.into_iter().map(|item| item.map(|v| {
      let bytes: &[u8] = v.as_ref();

      bytes
    })))
  }
}

#[derive(Clone)]
struct BytesLookupTable<Helper: BytesMapHelperWrapperTrait> {
  /// Map from non-null literal value the first occurrence index in the literals
  map: HashMap<Vec<u8>, i32>,
  null_index: i32,
  else_index: i32,

  _phantom_data: PhantomData<Helper>,
}

impl<T: BytesMapHelperWrapperTrait> Debug for BytesLookupTable<T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("BytesMapHelper")
      .field("map", &self.map)
      .field("null_index", &self.null_index)
      .field("else_index", &self.else_index)
      .finish()
  }
}

impl<Helper: BytesMapHelperWrapperTrait> LookupTable for BytesLookupTable<Helper> {
  fn try_new(literals: &[Arc<Literal>], else_index: i32) -> datafusion_common::Result<Self>
  where
    Self: Sized,
  {
    let input = ScalarValue::iter_to_array(literals.iter().map(|item| item.value().clone()))?;
    let bytes_iter = Helper::array_to_iter(&input)?;

    let mut null_index = None;

    let mut map: HashMap<Vec<u8>, i32> = HashMap::new();

    for (map_index, value) in bytes_iter.enumerate() {
      match value {
        Some(value) => {
          let slice_value: &[u8] = value.as_ref();

          // Insert only the first occurrence
          map.entry(slice_value.to_vec()).or_insert(map_index as i32);
        }
        None => {
          // Only set the null index once
          if null_index.is_none() {
            null_index = Some(map_index as i32);
          }
        }
      }
    }

    Ok(Self {
      map,
      null_index: null_index.unwrap_or(else_index),
      else_index,
      _phantom_data: Default::default(),
    })
  }

  fn match_values(&self, array: &ArrayRef) -> datafusion_common::Result<Vec<i32>> {
    let bytes_iter = Helper::array_to_iter(array)?;
    let indices = bytes_iter
      .map(|value| {
        match value {
          Some(value) => {
            let slice_value: &[u8] = value.as_ref();
            self.map.get(slice_value).copied().unwrap_or(self.else_index)
          }
          None => {
            self.null_index
          }
        }
      })
      .collect::<Vec<i32>>();

    Ok(indices)
  }
}


#[derive(Clone, Debug)]
struct BooleanLookupMap {
  true_index: i32,
  false_index: i32,
  null_index: i32,
}

impl LookupTable for BooleanLookupMap {
  fn try_new(literals: &[Arc<Literal>], else_index: i32) -> datafusion_common::Result<Self>
  where
    Self: Sized,
  {
    fn get_first_index(
      literals: &[Arc<Literal>],
      target: Option<bool>,
    ) -> Option<i32> {
      literals
        .iter()
        .position(|literal| matches!(literal.value(), ScalarValue::Boolean(target)))
        .map(|pos| pos as i32)
    }

    Ok(Self {
      false_index: get_first_index(literals, Some(false)).unwrap_or(else_index),
      true_index: get_first_index(literals, Some(true)).unwrap_or(else_index),
      null_index: get_first_index(literals, None).unwrap_or(else_index),
    })
  }

  fn match_values(&self, array: &ArrayRef) -> datafusion_common::Result<Vec<i32>> {
    Ok(
      array
        .as_boolean()
        .into_iter()
        .map(|value| match value {
          Some(true) => self.true_index,
          Some(false) => self.false_index,
          None => self.null_index,
        })
        .collect::<Vec<i32>>()
    )
  }
}

macro_rules! impl_lookup_table_super_traits {
    (impl _ for $MyType:ty) => {
        impl_lookup_table_super_traits!(impl<> _ for $MyType where);
    };
    (impl<$($impl_generics:ident),*> _ for $MyType:ty where $($where_clause:tt)*) => {
        impl<$($impl_generics),*> Debug for $MyType
        where
            $($where_clause)*
        {
            fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                f.debug_struct(stringify!($MyType))
                    .field("map", &self.map)
                    .field("else_index", &self.else_index)
                    .finish()
            }
        }
    };
}

impl_lookup_table_super_traits!(
    impl<T> _ for PrimitiveArrayMapHolder<T> where
        T: ArrowPrimitiveType,
        T::Native: ToHashableKey,
);

// TODO - We need to port it to arrow so that it can be reused in other places

/// Trait that help convert a value to a key that is hashable and equatable
/// This is needed as some types like f16/f32/f64 do not implement Hash/Eq directly
trait ToHashableKey: ArrowNativeTypeOp {
  /// The type that is hashable and equatable
  /// It must be an Arrow native type but it NOT GUARANTEED to be the same as Self
  /// this is just a helper trait so you can reuse the same code for all arrow native types
  type HashableKey: Hash + Eq + Debug + Clone + Copy + Send + Sync;

  /// Converts self to a hashable key
  /// the result of this value can be used as the key in hash maps/sets
  fn into_hashable_key(self) -> Self::HashableKey;
}

macro_rules! impl_to_hashable_key {
    (@single_already_hashable | $t:ty) => {
        impl ToHashableKey for $t {
            type HashableKey = $t;

            #[inline]
            fn into_hashable_key(self) -> Self::HashableKey {
                self
            }
        }
    };
    (@already_hashable | $($t:ty),+ $(,)?) => {
        $(
            impl_to_hashable_key!(@single_already_hashable | $t);
        )+
    };
    (@float | $t:ty => $hashable:ty) => {
        impl ToHashableKey for $t {
            type HashableKey = $hashable;

            #[inline]
            fn into_hashable_key(self) -> Self::HashableKey {
                self.to_bits()
            }
        }
    };
}

impl_to_hashable_key!(@already_hashable | i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, IntervalDayTime, IntervalMonthDayNano);
impl_to_hashable_key!(@float | f16 => u16);
impl_to_hashable_key!(@float | f32 => u32);
impl_to_hashable_key!(@float | f64 => u64);

#[cfg(test)]
mod tests {
  use super::ToHashableKey;
  use arrow::array::downcast_primitive;

  // This test ensure that all arrow primitive types implement ToHashableKey
  // otherwise the code will not compile
  #[test]
  fn should_implement_to_hashable_key_for_all_primitives() {
    #[derive(Debug, Default)]
    struct ExampleSet<T>
    where
      T: arrow::datatypes::ArrowPrimitiveType,
      T::Native: ToHashableKey,
    {
      _map: std::collections::HashSet<<T::Native as ToHashableKey>::HashableKey>,
    }

    macro_rules! create_matching_set {
            ($t:ty) => {{
                let _lookup_table = ExampleSet::<$t> {
                    _map: Default::default()
                };

                return;
            }};
        }

    let data_type = arrow::datatypes::DataType::Float16;

    downcast_primitive! {
            data_type => (create_matching_set),
            _ => panic!("not implemented for {data_type}"),
        }
  }
}
