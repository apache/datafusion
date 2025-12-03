

pub trait CollectBool {

  fn collect_bool<F: FnMut(usize) -> bool>(length: usize, rest: bool, f: F) -> Self;
  fn collect_bool_full<F: FnMut(usize) -> bool>(f: F) -> Self;

  /// Collect booleans for partial length, filling the rest with `rest` value
  fn collect_bool_partial<F: FnMut(usize) -> bool>(length: usize, rest: bool, f: F) -> Self;

  fn get_bit(self, index: usize) -> bool;
}

impl CollectBool for u64 {
  fn collect_bool<F: FnMut(usize) -> bool>(length: usize, rest: bool, f: F) -> Self {
    if length >= 64 {
      Self::collect_bool_full(f)
    } else {
      Self::collect_bool_partial(length, rest, f)
    }
  }

  fn collect_bool_full<F: FnMut(usize) -> bool>(mut f: F) -> Self {
    let mut packed = 0;
    for bit_idx in 0..64 {
      packed |= (f(bit_idx) as u64) << bit_idx;
    }

    packed
  }
  fn collect_bool_partial<F: FnMut(usize) -> bool>(length: usize, rest: bool, f: F) -> Self {
    let mut packed = 0;
    let mut f = f;
    for bit_idx in 0..length {
      packed |= (f(bit_idx) as u64) << bit_idx;
    }

    // TODO - this can be done in a single operation
    for bit_idx in length..64 {
      packed |= (rest as u64) << bit_idx;
    }

    packed
  }

  fn get_bit(self, index: usize) -> bool {
    ((self >> index) & 1) != 0
  }
}
