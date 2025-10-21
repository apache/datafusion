mod boolean_lookup_table;
mod bytes_like_lookup_table;
mod primitive_lookup_table;

use datafusion_common::DataFusionError;
use crate::expressions::case::literal_lookup_table::boolean_lookup_table::BooleanIndexMap;
use crate::expressions::case::literal_lookup_table::bytes_like_lookup_table::{
  BytesDictionaryHelper, BytesLikeIndexMap, BytesViewDictionaryHelper,
  FixedBinaryHelper, FixedBytesDictionaryHelper, GenericBytesHelper,
  GenericBytesViewHelper,
};
use crate::expressions::case::literal_lookup_table::primitive_lookup_table::PrimitiveArrayMapHolder;
use arrow::array::{downcast_integer, downcast_primitive, ArrayRef, Int32Array};
use arrow::datatypes::{
    ArrowDictionaryKeyType, BinaryViewType, DataType, GenericBinaryType,
    GenericStringType, StringViewType,
};
use datafusion_common::{arrow_datafusion_err, plan_datafusion_err, ScalarValue};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use crate::expressions::case::WhenThen;
use crate::expressions::Literal;

/// Optimization for CASE expressions with literal WHEN and THEN clauses
///
/// for this form:
/// ```sql
/// CASE <expr_a>
///     WHEN <literal_a> THEN <literal_e>
///     WHEN <literal_b> THEN <literal_f>
///     WHEN <literal_c> THEN <literal_g>
///     WHEN <literal_d> THEN <literal_h>
///     ELSE <optional-fallback_literal>
/// END
/// ```
///
/// # Improvement idea
/// TODO - we should think of unwrapping the `IN` expressions into multiple equality comparisons
/// so it will use this optimization as well, e.g.
/// ```sql
/// -- Before
/// CASE
///     WHEN (<expr_a> = <literal_a>) THEN <literal_e>
///     WHEN (<expr_a> in (<literal_b>, <literal_c>) THEN <literal_f>
///     WHEN (<expr_a> = <literal_d>) THEN <literal_g>
/// ELSE <optional-fallback_literal>
///
/// -- After
/// CASE
///     WHEN (<expr_a> = <literal_a>) THEN <literal_e>
///     WHEN (<expr_a> = <literal_b>) THEN <literal_f>
///     WHEN (<expr_a> = <literal_c>) THEN <literal_g>
///     WHEN (<expr_a> = <literal_d>) THEN <literal_h>
///     ELSE <optional-fallback_literal>
/// END
/// ```
///
#[derive(Debug)]
pub(in super::super) struct LiteralLookupTable {
  /// The lookup table to use for evaluating the CASE expression
  lookup: Arc<dyn WhenLiteralIndexMap>,

  /// ArrayRef where array[i] = then_literals[i]
  /// the last value in the array is the else_expr
  values_to_take_from: ArrayRef,
}

impl Hash for LiteralLookupTable {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    // Hashing the pointer as this is the best we can do here

    let lookup_ptr = Arc::as_ptr(&self.lookup);
    lookup_ptr.hash(state);

    let values_ptr = Arc::as_ptr(&self.lookup);
    values_ptr.hash(state);
  }
}

impl PartialEq for LiteralLookupTable {
  fn eq(&self, other: &Self) -> bool {
    // Comparing the pointers as this is the best we can do here
    Arc::ptr_eq(&self.lookup, &other.lookup) &&
      self.values_to_take_from.as_ref() == other.values_to_take_from.as_ref()
  }
}

impl Eq for LiteralLookupTable {

}

impl LiteralLookupTable {
  pub(in super::super) fn maybe_new(
    when_then_expr: &Vec<WhenThen>, else_expr: &Option<Arc<dyn PhysicalExpr>>
  ) -> Option<Self> {
    // We can't use the optimization if we don't have any when then pairs
    if when_then_expr.is_empty() {
      return None;
    }

    // If we only have 1 than this optimization is not useful
    if when_then_expr.len() == 1 {
      return None;
    }

    let when_then_exprs_maybe_literals = when_then_expr
      .iter()
      .map(|(when, then)| {
        let when_maybe_literal = when.as_any().downcast_ref::<Literal>();
        let then_maybe_literal = then.as_any().downcast_ref::<Literal>();

        when_maybe_literal.zip(then_maybe_literal)
      })
      .collect::<Vec<_>>();

    // If not all the when/then expressions are literals we cannot use this optimization
    if when_then_exprs_maybe_literals.contains(&None) {
      return None;
    }

    let (when_literals, then_literals): (Vec<ScalarValue>, Vec<ScalarValue>) = when_then_exprs_maybe_literals
      .iter()
      // Unwrap the options as we have already checked they are all Some
      .flatten()
      .map(|(when_lit, then_lit)| (when_lit.value().clone(), then_lit.value().clone()))
      .unzip();


    let else_expr: ScalarValue = if let Some(else_expr) = else_expr {
      let literal = else_expr.as_any().downcast_ref::<Literal>()?;

      literal.value().clone()
    } else {
      let Ok(null_scalar) = ScalarValue::try_new_null(&then_literals[0].data_type())
      else {
        return None;
      };

      null_scalar
    };

    {
      let data_type = when_literals[0].data_type();

      // If not all the when literals are the same data type we cannot use this optimization
      if when_literals.iter().any(|l| l.data_type() != data_type) {
        return None;
      }
    }

    {
      let data_type = then_literals[0].data_type();

      // If not all the then and the else literals are the same data type we cannot use this optimization
      if then_literals.iter().any(|l| l.data_type() != data_type) {
        return None;
      }

      if else_expr.data_type() != data_type {
        return None;
      }
    }


    let output_array = ScalarValue::iter_to_array(
      then_literals.iter()
        // The else is in the end
        .chain(std::iter::once(&else_expr))
        .cloned()
    ).ok()?;

    let lookup = try_creating_lookup_table(
      when_literals,

      // The else expression is in the end
      output_array.len() as i32 - 1,
    ).ok()?;

    Some(Self {
      lookup,
      values_to_take_from: output_array,
    })
  }

  pub(in super::super) fn create_output(&self, expr_array: &ArrayRef) -> datafusion_common::Result<ArrayRef> {
    let take_indices = self.lookup.match_values(expr_array)?;

    // Zero-copy conversion
    let take_indices = Int32Array::from(take_indices);

    // An optimize version would depend on the type of the values_to_take_from
    // For example, if the type is view we can just keep pointing to the same value (similar to dictionary)
    // if the type is dictionary we can just use the indices as is (or cast them to the key type) and create a new dictionary array
    let output = arrow::compute::take(&self.values_to_take_from, &take_indices, None)
      .map_err(|e| arrow_datafusion_err!(e))?;

    Ok(output)
  }
}

/// Lookup table for mapping literal values to their corresponding indices in the THEN clauses
///
/// The else index is used when a value is not found in the lookup table
pub(super) trait WhenLiteralIndexMap: Debug + Send + Sync {
    /// Try creating a new lookup table from the given literals and else index
    fn try_new(
        literals: Vec<ScalarValue>,
        else_index: i32,
    ) -> datafusion_common::Result<Self>
    where
        Self: Sized;

    /// Return indices to take from the literals based on the values in the given array
    fn match_values(&self, array: &ArrayRef) -> datafusion_common::Result<Vec<i32>>;
}

pub(crate) fn try_creating_lookup_table(
    literals: Vec<ScalarValue>,
    else_index: i32,
) -> datafusion_common::Result<Arc<dyn WhenLiteralIndexMap>> {
    assert_ne!(literals.len(), 0, "Must have at least one literal");
    match literals[0].data_type() {
        DataType::Boolean => {
            let lookup_table = BooleanIndexMap::try_new(literals, else_index)?;
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
            let lookup_table = BytesLikeIndexMap::<
                GenericBytesHelper<GenericStringType<i32>>,
            >::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }

        DataType::LargeUtf8 => {
            let lookup_table = BytesLikeIndexMap::<
                GenericBytesHelper<GenericStringType<i64>>,
            >::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }

        DataType::Binary => {
            let lookup_table = BytesLikeIndexMap::<
                GenericBytesHelper<GenericBinaryType<i32>>,
            >::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }

        DataType::LargeBinary => {
            let lookup_table = BytesLikeIndexMap::<
                GenericBytesHelper<GenericBinaryType<i64>>,
            >::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }

        DataType::FixedSizeBinary(_) => {
            let lookup_table =
                BytesLikeIndexMap::<FixedBinaryHelper>::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }

        DataType::Utf8View => {
            let lookup_table =
                BytesLikeIndexMap::<GenericBytesViewHelper<StringViewType>>::try_new(
                    literals, else_index,
                )?;
            Ok(Arc::new(lookup_table))
        }
        DataType::BinaryView => {
            let lookup_table =
                BytesLikeIndexMap::<GenericBytesViewHelper<BinaryViewType>>::try_new(
                    literals, else_index,
                )?;
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
) -> datafusion_common::Result<Arc<dyn WhenLiteralIndexMap>> {

  // TODO - optimize dictionary to use different wrapper that takes advantage of it being a dictionary
    match value {
        DataType::Utf8 => {
            let lookup_table = BytesLikeIndexMap::<
                BytesDictionaryHelper<K, GenericStringType<i32>>,
            >::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }

        DataType::LargeUtf8 => {
            let lookup_table = BytesLikeIndexMap::<
                BytesDictionaryHelper<K, GenericStringType<i64>>,
            >::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }

        DataType::Binary => {
            let lookup_table = BytesLikeIndexMap::<
                BytesDictionaryHelper<K, GenericBinaryType<i32>>,
            >::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }

        DataType::LargeBinary => {
            let lookup_table = BytesLikeIndexMap::<
                BytesDictionaryHelper<K, GenericBinaryType<i64>>,
            >::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }

        DataType::FixedSizeBinary(_) => {
            let lookup_table =
                BytesLikeIndexMap::<FixedBytesDictionaryHelper<K>>::try_new(
                    literals, else_index,
                )?;
            Ok(Arc::new(lookup_table))
        }

        DataType::Utf8View => {
            let lookup_table = BytesLikeIndexMap::<
                BytesViewDictionaryHelper<K, StringViewType>,
            >::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }
        DataType::BinaryView => {
            let lookup_table = BytesLikeIndexMap::<
                BytesViewDictionaryHelper<K, BinaryViewType>,
            >::try_new(literals, else_index)?;
            Ok(Arc::new(lookup_table))
        }
        _ => Err(plan_datafusion_err!(
            "Unsupported dictionary value type for lookup table: {}",
            value
        )),
    }
}
