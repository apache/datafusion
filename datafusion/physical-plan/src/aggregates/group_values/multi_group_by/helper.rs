use arrow::{array::ArrayRef, buffer::NullBuffer, util::bit_util::{self, apply_bitwise_unary_op}};

use crate::aggregates::group_values::{multi_group_by::FixedBitPackedMutableBuffer, null_builder::MaybeNullBufferBuilder};



pub trait CollectBool {

  fn collect_bool<const REST: bool, F: FnMut(usize) -> bool>(length: usize, f: F) -> Self;
  fn collect_bool_full<F: FnMut(usize) -> bool>(f: F) -> Self;

  /// Collect booleans for partial length, filling the rest with `rest` value
  fn collect_bool_partial<const REST: bool, F: FnMut(usize) -> bool>(length: usize, f: F) -> Self;

  fn get_bit(self, index: usize) -> bool;
}

impl CollectBool for u64 {
  #[inline]
  fn collect_bool<const REST: bool, F: FnMut(usize) -> bool>(length: usize, f: F) -> Self {
    if length >= 64 {
      Self::collect_bool_full(f)
    } else {
      Self::collect_bool_partial::<REST, _>(length, f)
    }
  }

  #[inline]
  fn collect_bool_full<F: FnMut(usize) -> bool>(mut f: F) -> Self {
    let mut packed = 0;
    for bit_idx in 0..64 {
      packed |= (f(bit_idx) as u64) << bit_idx;
    }

    packed
  }
  
  #[inline]
  fn collect_bool_partial<const REST: bool, F: FnMut(usize) -> bool>(length: usize, f: F) -> Self {
    let mut packed = 0;
    let mut f = f;
    for bit_idx in 0..length {
      packed |= (f(bit_idx) as u64) << bit_idx;
    }
    
    if REST {
        packed |= u64::MAX << length
    }

    packed
  }

  #[inline]
  fn get_bit(self, index: usize) -> bool {
    ((self >> index) & 1) != 0
  }
}


/// Return (bit packed equal nullability, bit packed for both non-nulls)
/// 
/// the value in bits after length should not be used and is not gurrentee to any value 
/// 
/// so if comparing both:
/// (F - null, T - valid)
/// ```text
/// [F, F, T, T, F]
/// [F, T, T, F, F]
/// ```
/// 
/// it will return bit packed for this:
/// (F - unset, T - set)
/// ```text
/// [T, F, T, F, T] for equal nullability 
/// [F, F, T, F, F] for both non nulls
/// ```
pub(crate) fn compare_nulls_to_packed(
    length: usize,
    offset: usize,
    lhs_rows: &[usize],
    lhs_nulls: &MaybeNullBufferBuilder,
    rhs_rows: &[usize],
    rhs_nulls: Option<&NullBuffer>,
) -> (u64, u64) {
    let selected_self_nulls_packed  = if lhs_nulls.might_have_nulls() {
        u64::collect_bool::<
            // rest here doesn't matter as it should not be used
            false,
            _
        >(
            length,
            |bit_idx| {
                let current_index = offset + bit_idx;
                let lhs_row = if cfg!(debug_assertions) {
                    lhs_rows[current_index]
                } else {
                    // SAFETY: indices are guaranteed to be in bounds
                    unsafe {*lhs_rows.get_unchecked(current_index)}
                };
                
                lhs_nulls.is_valid(lhs_row)
            },
        )
    } else {
        u64::MAX
    };
    
    let selected_array_nulls_packed = if let Some(nulls) = rhs_nulls {
        u64::collect_bool::<
            // rest here doesn't matter as it should not be used
            false,
            _
        >(
            length,
            |bit_idx| {
                let current_index = offset + bit_idx;
                let rhs_row = if cfg!(debug_assertions) {
                    rhs_rows[current_index]
                } else {
                    // SAFETY: indices are guaranteed to be in bounds
                    unsafe {*rhs_rows.get_unchecked(current_index)}
                };
                
                // TODO - should use here unchecked as well?
                nulls.is_valid(rhs_row)
            },
        )
    } else {
        // all valid
        u64::MAX
    };
    
    (
        // Equal nullability if both false or true, than this is true
        !(selected_self_nulls_packed ^ selected_array_nulls_packed),
        // For both valid,
        selected_self_nulls_packed & selected_array_nulls_packed
    )
}


/// Return (bit packed equal nullability, bit packed for both non-nulls)
/// 
/// the value in bits after length should not be used and is not gurrentee to any value 
/// 
/// so if comparing both:
/// (F - null, T - valid)
/// ```text
/// [F, F, T, T, F]
/// [F, T, T, F, F]
/// ```
/// 
/// it will return bit packed for this:
/// (F - unset, T - set)
/// ```text
/// [T, F, T, F, T] for equal nullability 
/// [F, F, T, F, F] for both non nulls
/// ```
pub(crate) fn compare_fixed_nulls_to_packed(
    length: usize,
    lhs_rows: &[usize; 64],
    lhs_nulls: Option<&[u8]>,
    rhs_rows: &[usize; 64],
    rhs_nulls: Option<&NullBuffer>,
) -> (u64, u64) {
    let selected_self_nulls_packed  = if let Some(lhs_nulls) = lhs_nulls {
        let lhs_nulls_ptr =lhs_nulls.as_ptr();
        u64::collect_bool::<
            // rest here doesn't matter as it should not be used
            false,
            _
        >(
            length,
            |bit_idx| {
                let lhs_row = if cfg!(debug_assertions) {
                    lhs_rows[bit_idx]
                } else {
                    // SAFETY: indices are guaranteed to be in bounds
                    unsafe {*lhs_rows.get_unchecked(bit_idx)}
                };
                
                unsafe { bit_util::get_bit_raw(lhs_nulls_ptr, lhs_row) }
            },
        )
    } else {
        u64::MAX
    };
    
    let selected_array_nulls_packed = if let Some(nulls) = rhs_nulls {
        let rhs_nulls_values = nulls.inner().values();
        let offset = nulls.offset();
        let rhs_nulls_ptr = rhs_nulls_values.as_ptr();
        
        u64::collect_bool::<
            // rest here doesn't matter as it should not be used
            false,
            _
        >(
            length,
            |bit_idx| {
                let rhs_row = if cfg!(debug_assertions) {
                    rhs_rows[bit_idx]
                } else {
                    // SAFETY: indices are guaranteed to be in bounds
                    unsafe {*rhs_rows.get_unchecked(bit_idx)}
                };
                
                unsafe { bit_util::get_bit_raw(rhs_nulls_ptr, offset + rhs_row) }
            },
        )
    } else {
        // all valid
        u64::MAX
    };
    
    (
        // Equal nullability if both false or true, than this is true
        !(selected_self_nulls_packed ^ selected_array_nulls_packed),
        // For both valid,
        selected_self_nulls_packed & selected_array_nulls_packed
    )
}


/// Return (bit packed equal nullability, bit packed for both non-nulls)
/// 
/// the value in bits after length should not be used and is not gurrentee to any value 
/// 
/// so if comparing both:
/// (F - null, T - valid)
/// ```text
/// [F, F, T, T, F]
/// [F, T, T, F, F]
/// ```
/// 
/// it will return bit packed for this:
/// (F - unset, T - set)
/// ```text
/// [T, F, T, F, T] for equal nullability 
/// [F, F, T, F, F] for both non nulls
/// ```
pub(crate) fn compare_fixed_raw_nulls_to_packed(
    length: usize,
    lhs_rows: &[usize; 64],
    lhs_nulls: Option<&[u8]>,
    rhs_rows: &[usize; 64],
    rhs_nulls: Option<(
        // offset
        usize,
        // buffer
        &[u8]
    )>,
) -> (u64, u64) {
    let selected_self_nulls_packed  = if let Some(lhs_nulls) = lhs_nulls {
        let lhs_nulls_ptr =lhs_nulls.as_ptr();
        u64::collect_bool::<
            // rest here doesn't matter as it should not be used
            false,
            _
        >(
            length,
            |bit_idx| {
                let lhs_row = if cfg!(debug_assertions) {
                    lhs_rows[bit_idx]
                } else {
                    // SAFETY: indices are guaranteed to be in bounds
                    unsafe {*lhs_rows.get_unchecked(bit_idx)}
                };
                
                unsafe { bit_util::get_bit_raw(lhs_nulls_ptr, lhs_row) }
            },
        )
    } else {
        u64::MAX
    };
    
    let selected_array_nulls_packed = if let Some((offset, nulls)) = rhs_nulls {
        let rhs_nulls_ptr = nulls.as_ptr();
        
        u64::collect_bool::<
            // rest here doesn't matter as it should not be used
            false,
            _
        >(
            length,
            |bit_idx| {
                let rhs_row = if cfg!(debug_assertions) {
                    rhs_rows[bit_idx]
                } else {
                    // SAFETY: indices are guaranteed to be in bounds
                    unsafe {*rhs_rows.get_unchecked(bit_idx)}
                };
                
                unsafe { bit_util::get_bit_raw(rhs_nulls_ptr, offset + rhs_row) }
            },
        )
    } else {
        // all valid
        u64::MAX
    };
    
    (
        // Equal nullability if both false or true, than this is true
        !(selected_self_nulls_packed ^ selected_array_nulls_packed),
        // For both valid,
        selected_self_nulls_packed & selected_array_nulls_packed
    )
}



/// Return u64 bit packed where a bit is set if (both are nulls) || (both are valid && values eq)
/// 
/// Equal map
/// 
/// |   nulls   |val |        |
/// | lhs | rhs | eq | result |
/// | --------- | -- | ------ |
/// |  F  |  F  | F  | T      |
/// |  F  |  F  | T  | T      |
/// |  F  |  T  | F  | F      |
/// |  F  |  T  | T  | F      |
/// |  T  |  F  | F  | F      |
/// |  T  |  F  | T  | F      |
/// |  T  |  T  | F  | F      |
/// |  T  |  T  | T  | T      |
#[inline]
pub(crate) fn combine_nullability_and_value_equal_bit_packed_u64(
    both_valid: u64,
    nullability_eq: u64,
    values_eq: u64
) -> u64 {
    (both_valid & values_eq) | (nullability_eq & !both_valid)
}


// pub fn vectorized_equal_to_helper<const CHECK_NULLABILITY: bool>(
//     lhs_rows: &[usize],
//     rhs_rows: &[usize],
//     rhs_array: &ArrayRef,
//     equal_to_results: &mut FixedBitPackedMutableBuffer,
// ) {
//     if !CHECK_NULLABILITY {
//         assert!(
//             (rhs_array.null_count() == 0 && !self.nulls.might_have_nulls()),
//             "CHECK_NULLABILITY is false for nullable called with nullable input"
//         );
//     }

//     assert_eq!(lhs_rows.len(), rhs_rows.len());
//     assert_eq!(lhs_rows.len(), equal_to_results.len());

//     // TODO - skip to the first true bit in equal_to_results to avoid unnecessary work
//     //        in iterating over unnecessary bits oe even get a slice of starting from first true bit to the last true bit

//     // TODO - do not assume for byte aligned, added here just for POC
//     let mut index = 0;
//     let num_rows = lhs_rows.len();
//     apply_bitwise_unary_op(
//         equal_to_results.0.as_slice_mut(),
//         0,
//         lhs_rows.len(),
//         |eq| {
//             // If already false, skip 64 items
//             if eq == 0 {
//                 index += 64;
//                 return 0;
//             }
            
//             let length = num_rows - index;
            
//             let (nullability_eq, both_valid) = if CHECK_NULLABILITY {
//                 compare_nulls_to_packed(
//                     length,
//                     index,
//                     lhs_rows,
//                     &self.nulls,
//                     rhs_rows,
//                     array.nulls()
//                 )
//             } else {
//               (
//                   // nullability equal
//                   u64::MAX,
//                   // both valid
//                   u64::MAX
//               )  
//             };
            
//             if nullability_eq == 0 {
//                 index += 64;
//                 return 0
//             }
            
//             // if all nullability match and they both nulls than its the same value
//             if nullability_eq == u64::MAX && both_valid == 0 {
//                 index += 64;
//                 return eq;
//             }

//             // TODO - we can maybe get only from the first set bit until the last set bit
//             // and then update those gaps with false
//             // TODO - make sure not to override bits after `length`
//             let values_eq = self.get_bit_packed_u64_for_eq_values(
//                 length,
//                 index,
//                 lhs_rows,
//                 rhs_rows,
//                 array_values,
//             );
            
//             let result = combine_nullability_and_value_equal_bit_packed_u64(
//                 both_valid,
//                 nullability_eq,
//                 values_eq
//             );

//             index += 64;
//             eq & result
//         },
//     );
// }