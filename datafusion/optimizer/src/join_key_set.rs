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

//! [JoinKeySet] for tracking the set of join keys in a plan.

use datafusion_expr::Expr;
use indexmap::{Equivalent, IndexSet};

/// Tracks a set of equality Join keys
///
/// A join key is an expression that is used to join two tables via an equality
/// predicate such as `a.x = b.y`
///
/// This struct models `a.x + 5 = b.y AND a.z = b.z` as two join keys
/// 1. `(a.x + 5,  b.y)`
/// 2. `(a.z,      b.z)`
///
/// # Important properties:
///
/// 1. Retains insert order
/// 2. Can quickly look up if a pair of expressions are in the set.
#[derive(Debug)]
pub struct JoinKeySet {
    inner: IndexSet<(Expr, Expr)>,
}

impl JoinKeySet {
    /// Create a new empty set
    pub fn new() -> Self {
        Self {
            inner: IndexSet::new(),
        }
    }

    /// Return true if the set contains a join pair
    /// where left = right or right = left
    pub fn contains(&self, left: &Expr, right: &Expr) -> bool {
        self.inner.contains(&ExprPair::new(left, right))
            || self.inner.contains(&ExprPair::new(right, left))
    }

    /// Insert the join key `(left = right)` into the set  if join pair `(right =
    /// left)` is not already in the set
    ///
    /// returns true if the pair was inserted
    pub fn insert(&mut self, left: &Expr, right: &Expr) -> bool {
        if self.contains(left, right) {
            false
        } else {
            self.inner.insert((left.clone(), right.clone()));
            true
        }
    }

    /// Same as [`Self::insert`] but avoids cloning expression if they
    /// are owned
    pub fn insert_owned(&mut self, left: Expr, right: Expr) -> bool {
        if self.contains(&left, &right) {
            false
        } else {
            self.inner.insert((left, right));
            true
        }
    }

    /// Inserts potentially many join keys into the set, copying only when necessary
    ///
    /// returns true if any of the pairs were inserted
    pub fn insert_all<'a>(
        &mut self,
        iter: impl IntoIterator<Item = &'a (Expr, Expr)>,
    ) -> bool {
        let mut inserted = false;
        for (left, right) in iter.into_iter() {
            inserted |= self.insert(left, right);
        }
        inserted
    }

    /// Same as [`Self::insert_all`] but avoids cloning expressions if they are
    /// already owned
    ///
    /// returns true if any of the pairs were inserted
    pub fn insert_all_owned(
        &mut self,
        iter: impl IntoIterator<Item = (Expr, Expr)>,
    ) -> bool {
        let mut inserted = false;
        for (left, right) in iter.into_iter() {
            inserted |= self.insert_owned(left, right);
        }
        inserted
    }

    /// Inserts any join keys that are common to both `s1` and `s2` into self
    pub fn insert_intersection(&mut self, s1: &JoinKeySet, s2: &JoinKeySet) {
        // note can't use inner.intersection as we need to consider both (l, r)
        // and (r, l) in equality
        for (left, right) in s1.inner.iter() {
            if s2.contains(left, right) {
                self.insert(left, right);
            }
        }
    }

    /// returns true if this set is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return the length of this set
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return an iterator over the join keys in this set
    pub fn iter(&self) -> impl Iterator<Item = (&Expr, &Expr)> {
        self.inner.iter().map(|(l, r)| (l, r))
    }
}

/// Custom comparison operation to avoid copying owned values
///
/// This behaves like a `(Expr, Expr)` tuple for hashing and  comparison, but
/// avoids copying the values simply to comparing them.

#[derive(Debug, Eq, PartialEq, Hash)]
struct ExprPair<'a>(&'a Expr, &'a Expr);

impl<'a> ExprPair<'a> {
    fn new(left: &'a Expr, right: &'a Expr) -> Self {
        Self(left, right)
    }
}

impl<'a> Equivalent<(Expr, Expr)> for ExprPair<'a> {
    fn equivalent(&self, other: &(Expr, Expr)) -> bool {
        self.0 == &other.0 && self.1 == &other.1
    }
}

#[cfg(test)]
mod test {
    use crate::join_key_set::JoinKeySet;
    use datafusion_expr::{col, Expr};

    #[test]
    fn test_insert() {
        let mut set = JoinKeySet::new();
        // new sets should be empty
        assert!(set.is_empty());

        // insert (a = b)
        assert!(set.insert(&col("a"), &col("b")));
        assert!(!set.is_empty());

        // insert (a=b) again returns false
        assert!(!set.insert(&col("a"), &col("b")));
        assert_eq!(set.len(), 1);

        // insert (b = a) , should be considered equivalent
        assert!(!set.insert(&col("b"), &col("a")));
        assert_eq!(set.len(), 1);

        // insert (a = c) should be considered different
        assert!(set.insert(&col("a"), &col("c")));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_insert_owned() {
        let mut set = JoinKeySet::new();
        assert!(set.insert_owned(col("a"), col("b")));
        assert!(set.contains(&col("a"), &col("b")));
        assert!(set.contains(&col("b"), &col("a")));
        assert!(!set.contains(&col("a"), &col("c")));
    }

    #[test]
    fn test_contains() {
        let mut set = JoinKeySet::new();
        assert!(set.insert(&col("a"), &col("b")));
        assert!(set.contains(&col("a"), &col("b")));
        assert!(set.contains(&col("b"), &col("a")));
        assert!(!set.contains(&col("a"), &col("c")));

        assert!(set.insert(&col("a"), &col("c")));
        assert!(set.contains(&col("a"), &col("c")));
        assert!(set.contains(&col("c"), &col("a")));
    }

    #[test]
    fn test_iterator() {
        // put in c = a and
        let mut set = JoinKeySet::new();
        // put in c = a , b = c, and a = c and expect to get only the first 2
        set.insert(&col("c"), &col("a"));
        set.insert(&col("b"), &col("c"));
        set.insert(&col("a"), &col("c"));
        assert_contents(&set, vec![(&col("c"), &col("a")), (&col("b"), &col("c"))]);
    }

    #[test]
    fn test_insert_intersection() {
        // a = b, b = c, c = d
        let mut set1 = JoinKeySet::new();
        set1.insert(&col("a"), &col("b"));
        set1.insert(&col("b"), &col("c"));
        set1.insert(&col("c"), &col("d"));

        // a = a, b = b, b = c, d = c
        // should only intersect on b = c and c = d
        let mut set2 = JoinKeySet::new();
        set2.insert(&col("a"), &col("a"));
        set2.insert(&col("b"), &col("b"));
        set2.insert(&col("b"), &col("c"));
        set2.insert(&col("d"), &col("c"));

        let mut set = JoinKeySet::new();
        // put something in there already
        set.insert(&col("x"), &col("y"));
        set.insert_intersection(&set1, &set2);

        assert_contents(
            &set,
            vec![
                (&col("x"), &col("y")),
                (&col("b"), &col("c")),
                (&col("c"), &col("d")),
            ],
        );
    }

    fn assert_contents(set: &JoinKeySet, expected: Vec<(&Expr, &Expr)>) {
        let contents: Vec<_> = set.iter().collect();
        assert_eq!(contents, expected);
    }

    #[test]
    fn test_insert_all() {
        let mut set = JoinKeySet::new();

        // insert (a=b), (b=c), (b=a)
        set.insert_all(vec![
            &(col("a"), col("b")),
            &(col("b"), col("c")),
            &(col("b"), col("a")),
        ]);
        assert_eq!(set.len(), 2);
        assert!(set.contains(&col("a"), &col("b")));
        assert!(set.contains(&col("b"), &col("c")));
        assert!(set.contains(&col("b"), &col("a")));

        // should not contain (a=c)
        assert!(!set.contains(&col("a"), &col("c")));
    }

    #[test]
    fn test_insert_all_owned() {
        let mut set = JoinKeySet::new();

        // insert (a=b), (b=c), (b=a)
        set.insert_all_owned(vec![
            (col("a"), col("b")),
            (col("b"), col("c")),
            (col("b"), col("a")),
        ]);
        assert_eq!(set.len(), 2);
        assert!(set.contains(&col("a"), &col("b")));
        assert!(set.contains(&col("b"), &col("c")));
        assert!(set.contains(&col("b"), &col("a")));

        // should not contain (a=c)
        assert!(!set.contains(&col("a"), &col("c")));
    }
}
