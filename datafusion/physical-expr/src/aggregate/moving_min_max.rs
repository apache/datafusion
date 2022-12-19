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
                    self.pop_stack.push((val.clone(), val));
                    while let Some((val, _)) = self.push_stack.pop() {
                        // This is safe, because we just pushed one element onto
                        // pop_stack and therefore it cannot be empty.
                        let last = unsafe {
                            self.pop_stack.get_unchecked(self.pop_stack.len() - 1)
                        };
                        let min = if last.1 < val {
                            last.1.clone()
                        } else {
                            val.clone()
                        };
                        self.pop_stack.push((val.clone(), min));
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
                    self.pop_stack.push((val.clone(), val));
                    while let Some((val, _)) = self.push_stack.pop() {
                        // This is safe, because we just pushed one element onto
                        // pop_stack and therefore it cannot be empty.
                        let last = unsafe {
                            self.pop_stack.get_unchecked(self.pop_stack.len() - 1)
                        };
                        let max = if last.1 > val {
                            last.1.clone()
                        } else {
                            val.clone()
                        };
                        self.pop_stack.push((val.clone(), max));
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
