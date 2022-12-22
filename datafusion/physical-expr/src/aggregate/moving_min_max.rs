// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// The implementation is taken from https://github.com/spebern/moving_min_max/blob/master/src/lib.rs.

//! Keep track of the minimum or maximum value in a sliding window.
//!
//! `moving min max` provides one data structure for keeping track of the
//! minimum value and one for keeping track of the maximum value in a sliding
//! window.
//!
//! Each element is stored with the current min/max. One stack to push and another one for pop. If pop stack is empty,
//! push to this stack all elements popped from first stack while updating their current min/max. Now pop from
//! the second stack (MovingMin/Max struct works as a queue). To find the minimum element of the queue,
//! look at the smallest/largest two elements of the individual stacks, then take the minimum of those two values.
//!
//! The complexity of the operations are
//! - O(1) for getting the minimum/maximum
//! - O(1) for push
//! - amortized O(1) for pop

/// ```
/// # use datafusion_physical_expr::aggregate::moving_min_max::MovingMin;
/// let mut moving_min = MovingMin::<i32>::new();
/// moving_min.push(2);
/// moving_min.push(1);
/// moving_min.push(3);
///
/// assert_eq!(moving_min.min(), Some(&1));
/// assert_eq!(moving_min.pop(), Some(2));
///
/// assert_eq!(moving_min.min(), Some(&1));
/// assert_eq!(moving_min.pop(), Some(1));
///
/// assert_eq!(moving_min.min(), Some(&3));
/// assert_eq!(moving_min.pop(), Some(3));
///
/// assert_eq!(moving_min.min(), None);
/// assert_eq!(moving_min.pop(), None);
/// ```
#[derive(Debug)]
pub struct MovingMin<T> {
    push_stack: Vec<(T, T)>,
    pop_stack: Vec<(T, T)>,
}

impl<T: Clone + PartialOrd> Default for MovingMin<T> {
    fn default() -> Self {
        Self {
            push_stack: Vec::new(),
            pop_stack: Vec::new(),
        }
    }
}

impl<T: Clone + PartialOrd> MovingMin<T> {
    /// Creates a new `MovingMin` to keep track of the minimum in a sliding
    /// window.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `MovingMin` to keep track of the minimum in a sliding
    /// window with `capacity` allocated slots.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            push_stack: Vec::with_capacity(capacity),
            pop_stack: Vec::with_capacity(capacity),
        }
    }

    /// Returns the minimum of the sliding window or `None` if the window is
    /// empty.
    #[inline]
    pub fn min(&self) -> Option<&T> {
        match (self.push_stack.last(), self.pop_stack.last()) {
            (None, None) => None,
            (Some((_, min)), None) => Some(min),
            (None, Some((_, min))) => Some(min),
            (Some((_, a)), Some((_, b))) => Some(if a < b { a } else { b }),
        }
    }

    /// Pushes a new element into the sliding window.
    #[inline]
    pub fn push(&mut self, val: T) {
        self.push_stack.push(match self.push_stack.last() {
            Some((_, min)) => {
                if val > *min {
                    (val, min.clone())
                } else {
                    (val.clone(), val)
                }
            }
            None => (val.clone(), val),
        });
    }

    /// Removes and returns the last value of the sliding window.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.pop_stack.is_empty() {
            match self.push_stack.pop() {
                Some((val, _)) => {
                    let mut last = (val.clone(), val);
                    self.pop_stack.push(last.clone());
                    while let Some((val, _)) = self.push_stack.pop() {
                        let min = if last.1 < val {
                            last.1.clone()
                        } else {
                            val.clone()
                        };
                        last = (val.clone(), min);
                        self.pop_stack.push(last.clone());
                    }
                }
                None => return None,
            }
        }
        self.pop_stack.pop().map(|(val, _)| val)
    }

    /// Returns the number of elements stored in the sliding window.
    #[inline]
    pub fn len(&self) -> usize {
        self.push_stack.len() + self.pop_stack.len()
    }

    /// Returns `true` if the moving window contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
/// ```
/// # use datafusion_physical_expr::aggregate::moving_min_max::MovingMax;
/// let mut moving_max = MovingMax::<i32>::new();
/// moving_max.push(2);
/// moving_max.push(3);
/// moving_max.push(1);
///
/// assert_eq!(moving_max.max(), Some(&3));
/// assert_eq!(moving_max.pop(), Some(2));
///
/// assert_eq!(moving_max.max(), Some(&3));
/// assert_eq!(moving_max.pop(), Some(3));
///
/// assert_eq!(moving_max.max(), Some(&1));
/// assert_eq!(moving_max.pop(), Some(1));
///
/// assert_eq!(moving_max.max(), None);
/// assert_eq!(moving_max.pop(), None);
/// ```
#[derive(Debug)]
pub struct MovingMax<T> {
    push_stack: Vec<(T, T)>,
    pop_stack: Vec<(T, T)>,
}

impl<T: Clone + PartialOrd> Default for MovingMax<T> {
    fn default() -> Self {
        Self {
            push_stack: Vec::new(),
            pop_stack: Vec::new(),
        }
    }
}

impl<T: Clone + PartialOrd> MovingMax<T> {
    /// Creates a new `MovingMax` to keep track of the maximum in a sliding window.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `MovingMax` to keep track of the maximum in a sliding window with
    /// `capacity` allocated slots.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            push_stack: Vec::with_capacity(capacity),
            pop_stack: Vec::with_capacity(capacity),
        }
    }

    /// Returns the maximum of the sliding window or `None` if the window is empty.
    #[inline]
    pub fn max(&self) -> Option<&T> {
        match (self.push_stack.last(), self.pop_stack.last()) {
            (None, None) => None,
            (Some((_, max)), None) => Some(max),
            (None, Some((_, max))) => Some(max),
            (Some((_, a)), Some((_, b))) => Some(if a > b { a } else { b }),
        }
    }

    /// Pushes a new element into the sliding window.
    #[inline]
    pub fn push(&mut self, val: T) {
        self.push_stack.push(match self.push_stack.last() {
            Some((_, max)) => {
                if val < *max {
                    (val, max.clone())
                } else {
                    (val.clone(), val)
                }
            }
            None => (val.clone(), val),
        });
    }

    /// Removes and returns the last value of the sliding window.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.pop_stack.is_empty() {
            match self.push_stack.pop() {
                Some((val, _)) => {
                    let mut last = (val.clone(), val);
                    self.pop_stack.push(last.clone());
                    while let Some((val, _)) = self.push_stack.pop() {
                        let max = if last.1 > val {
                            last.1.clone()
                        } else {
                            val.clone()
                        };
                        last = (val.clone(), max);
                        self.pop_stack.push(last.clone());
                    }
                }
                None => return None,
            }
        }
        self.pop_stack.pop().map(|(val, _)| val)
    }

    /// Returns the number of elements stored in the sliding window.
    #[inline]
    pub fn len(&self) -> usize {
        self.push_stack.len() + self.pop_stack.len()
    }

    /// Returns `true` if the moving window contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::Result;
    use rand::Rng;

    fn get_random_vec_i32(len: usize) -> Vec<i32> {
        let mut rng = rand::thread_rng();
        let mut input = Vec::with_capacity(len);
        for _i in 0..len {
            input.push(rng.gen_range(0..100));
        }
        input
    }

    fn moving_min_i32(len: usize, n_sliding_window: usize) -> Result<()> {
        let data = get_random_vec_i32(len);
        let mut expected = Vec::with_capacity(len);
        let mut moving_min = MovingMin::<i32>::new();
        let mut res = Vec::with_capacity(len);
        for i in 0..len {
            let start = i.saturating_sub(n_sliding_window);
            expected.push(*data[start..i + 1].iter().min().unwrap());

            moving_min.push(data[i]);
            if i > n_sliding_window {
                moving_min.pop();
            }
            res.push(*moving_min.min().unwrap());
        }
        assert_eq!(res, expected);
        Ok(())
    }

    fn moving_max_i32(len: usize, n_sliding_window: usize) -> Result<()> {
        let data = get_random_vec_i32(len);
        let mut expected = Vec::with_capacity(len);
        let mut moving_max = MovingMax::<i32>::new();
        let mut res = Vec::with_capacity(len);
        for i in 0..len {
            let start = i.saturating_sub(n_sliding_window);
            expected.push(*data[start..i + 1].iter().max().unwrap());

            moving_max.push(data[i]);
            if i > n_sliding_window {
                moving_max.pop();
            }
            res.push(*moving_max.max().unwrap());
        }
        assert_eq!(res, expected);
        Ok(())
    }

    #[test]
    fn moving_min_tests() -> Result<()> {
        moving_min_i32(100, 10)?;
        moving_min_i32(100, 20)?;
        moving_min_i32(100, 50)?;
        moving_min_i32(100, 100)?;
        Ok(())
    }

    #[test]
    fn moving_max_tests() -> Result<()> {
        moving_max_i32(100, 10)?;
        moving_max_i32(100, 20)?;
        moving_max_i32(100, 50)?;
        moving_max_i32(100, 100)?;
        Ok(())
    }
}
