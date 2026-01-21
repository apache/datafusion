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

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Weak},
};

use parking_lot::Mutex;

#[derive(Default)]
/// Provides a Least Recently Used queue with unbounded capacity.
///
/// # Examples
///
/// ```
/// use datafusion_execution::cache::lru_queue::LruQueue;
///
/// let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();
/// lru_queue.put(1, 10);
/// lru_queue.put(2, 20);
/// lru_queue.put(3, 30);
/// assert_eq!(lru_queue.get(&2), Some(&20));
/// assert_eq!(lru_queue.pop(), Some((1, 10)));
/// assert_eq!(lru_queue.pop(), Some((3, 30)));
/// assert_eq!(lru_queue.pop(), Some((2, 20)));
/// assert_eq!(lru_queue.pop(), None);
/// ```
pub struct LruQueue<K: Eq + Hash + Clone, V> {
    data: LruData<K, V>,
    queue: LruList<K>,
}

/// Maps the key to the [`LruNode`] in queue and the value.
type LruData<K, V> = HashMap<K, (Arc<Mutex<LruNode<K>>>, V)>;

#[derive(Default)]
/// Doubly-linked list that maintains the LRU order
struct LruList<K> {
    head: Link<K>,
    tail: Link<K>,
}

/// Doubly-linked list node.
struct LruNode<K> {
    key: K,
    prev: Link<K>,
    next: Link<K>,
}

/// Weak pointer to a [`LruNode`], used to connect nodes in the doubly-linked list.
/// The strong reference is guaranteed to be stored in the `data` map of the [`LruQueue`].
type Link<K> = Option<Weak<Mutex<LruNode<K>>>>;

impl<K: Eq + Hash + Clone, V> LruQueue<K, V> {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            queue: LruList {
                head: None,
                tail: None,
            },
        }
    }

    /// Returns a reference to value mapped by `key`, if it exists.
    /// If the entry exists, it becomes the most recently used.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(value) = self.remove(key) {
            self.put(key.clone(), value);
        }
        self.data.get(key).map(|(_, value)| value)
    }

    /// Returns a reference to value mapped by `key`, if it exists.
    /// Does not affect the queue order.
    pub fn peek(&self, key: &K) -> Option<&V> {
        self.data.get(key).map(|(_, value)| value)
    }

    /// Checks whether there is an entry with key `key` in the queue.
    /// Does not affect the queue order.
    pub fn contains_key(&self, key: &K) -> bool {
        self.data.contains_key(key)
    }

    /// Inserts an entry in the queue, becoming the most recently used.
    /// If the entry already exists, returns the previous value.
    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        let old_value = self.remove(&key);

        let node = Arc::new(Mutex::new(LruNode {
            key: key.clone(),
            prev: None,
            next: None,
        }));

        match self.queue.head {
            // queue is not empty
            Some(ref old_head) => {
                old_head
                    .upgrade()
                    .expect("value has been unexpectedly dropped")
                    .lock()
                    .prev = Some(Arc::downgrade(&node));
                node.lock().next = Some(Weak::clone(old_head));
                self.queue.head = Some(Arc::downgrade(&node));
            }
            // queue is empty
            _ => {
                self.queue.head = Some(Arc::downgrade(&node));
                self.queue.tail = Some(Arc::downgrade(&node));
            }
        }

        self.data.insert(key, (node, value));

        old_value
    }

    /// Removes and returns the least recently used value.
    /// Returns `None` if the queue is empty.
    pub fn pop(&mut self) -> Option<(K, V)> {
        let key_to_remove = self.queue.tail.as_ref().map(|n| {
            n.upgrade()
                .expect("value has been unexpectedly dropped")
                .lock()
                .key
                .clone()
        });
        if let Some(k) = key_to_remove {
            let value = self.remove(&k).unwrap(); // confirmed above that the entry exists
            Some((k, value))
        } else {
            None
        }
    }

    /// Removes a specific entry from the queue, if it exists.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some((old_node, old_value)) = self.data.remove(key) {
            let LruNode { key: _, prev, next } = &*old_node.lock();
            match (prev, next) {
                // single node in the queue
                (None, None) => {
                    self.queue.head = None;
                    self.queue.tail = None;
                }
                // removed the head node
                (None, Some(n)) => {
                    let n_strong =
                        n.upgrade().expect("value has been unexpectedly dropped");
                    n_strong.lock().prev = None;
                    self.queue.head = Some(Weak::clone(n));
                }
                // removed the tail node
                (Some(p), None) => {
                    let p_strong =
                        p.upgrade().expect("value has been unexpectedly dropped");
                    p_strong.lock().next = None;
                    self.queue.tail = Some(Weak::clone(p));
                }
                // removed a middle node
                (Some(p), Some(n)) => {
                    let n_strong =
                        n.upgrade().expect("value has been unexpectedly dropped");
                    let p_strong =
                        p.upgrade().expect("value has been unexpectedly dropped");
                    n_strong.lock().prev = Some(Weak::clone(p));
                    p_strong.lock().next = Some(Weak::clone(n));
                }
            };
            Some(old_value)
        } else {
            None
        }
    }

    /// Returns the number of entries in the queue.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Checks whether the queue has no items.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Removes all entries from the queue.
    pub fn clear(&mut self) {
        self.queue.head = None;
        self.queue.tail = None;
        self.data.clear();
    }

    /// Returns a reference to the entries currently in the queue.
    pub fn list_entries(&self) -> HashMap<&K, &V> {
        self.data.iter().map(|(k, (_, v))| (k, v)).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::seq::IndexedRandom;

    use crate::cache::lru_queue::LruQueue;

    #[test]
    fn test_get() {
        let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();

        // value does not exist
        assert_eq!(lru_queue.get(&1), None);

        // value exists
        lru_queue.put(1, 10);
        assert_eq!(lru_queue.get(&1), Some(&10));
        assert_eq!(lru_queue.get(&1), Some(&10));

        // value is removed
        lru_queue.remove(&1);
        assert_eq!(lru_queue.get(&1), None);
    }

    #[test]
    fn test_peek() {
        let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();

        // value does not exist
        assert_eq!(lru_queue.peek(&1), None);

        // value exists
        lru_queue.put(1, 10);
        assert_eq!(lru_queue.peek(&1), Some(&10));
        assert_eq!(lru_queue.peek(&1), Some(&10));

        // value is removed
        lru_queue.remove(&1);
        assert_eq!(lru_queue.peek(&1), None);
    }

    #[test]
    fn test_put() {
        let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();

        // no previous value
        assert_eq!(lru_queue.put(1, 10), None);

        // update, the previous value is returned
        assert_eq!(lru_queue.put(1, 11), Some(10));
        assert_eq!(lru_queue.put(1, 12), Some(11));
        assert_eq!(lru_queue.put(1, 13), Some(12));
    }

    #[test]
    fn test_remove() {
        let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();

        // value does not exist
        assert_eq!(lru_queue.remove(&1), None);

        // value exists and is returned
        lru_queue.put(1, 10);
        assert_eq!(lru_queue.remove(&1), Some(10));

        // value does not exist
        assert_eq!(lru_queue.remove(&1), None);
    }

    #[test]
    fn test_contains_key() {
        let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();

        // value does not exist
        assert!(!lru_queue.contains_key(&1));

        // value exists
        lru_queue.put(1, 10);
        assert!(lru_queue.contains_key(&1));

        // value is removed
        lru_queue.remove(&1);
        assert!(!lru_queue.contains_key(&1));
    }

    #[test]
    fn test_len() {
        let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();

        // empty
        assert_eq!(lru_queue.len(), 0);

        // puts
        lru_queue.put(1, 10);
        assert_eq!(lru_queue.len(), 1);
        lru_queue.put(2, 20);
        assert_eq!(lru_queue.len(), 2);
        lru_queue.put(3, 30);
        assert_eq!(lru_queue.len(), 3);
        lru_queue.put(1, 11);
        lru_queue.put(3, 31);
        assert_eq!(lru_queue.len(), 3);

        // removes
        lru_queue.remove(&1);
        assert_eq!(lru_queue.len(), 2);
        lru_queue.remove(&1);
        assert_eq!(lru_queue.len(), 2);
        lru_queue.remove(&4);
        assert_eq!(lru_queue.len(), 2);
        lru_queue.remove(&3);
        assert_eq!(lru_queue.len(), 1);
        lru_queue.remove(&2);
        assert_eq!(lru_queue.len(), 0);
        lru_queue.remove(&2);
        assert_eq!(lru_queue.len(), 0);

        // clear
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        assert_eq!(lru_queue.len(), 3);
        lru_queue.clear();
        assert_eq!(lru_queue.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();

        // empty
        assert!(lru_queue.is_empty());

        // puts
        lru_queue.put(1, 10);
        assert!(!lru_queue.is_empty());
        lru_queue.put(2, 20);
        assert!(!lru_queue.is_empty());

        // removes
        lru_queue.remove(&1);
        assert!(!lru_queue.is_empty());
        lru_queue.remove(&1);
        assert!(!lru_queue.is_empty());
        lru_queue.remove(&2);
        assert!(lru_queue.is_empty());

        // clear
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        assert!(!lru_queue.is_empty());
        lru_queue.clear();
        assert!(lru_queue.is_empty());
    }

    #[test]
    fn test_clear() {
        let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();

        // empty
        lru_queue.clear();

        // filled
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        assert_eq!(lru_queue.get(&1), Some(&10));
        assert_eq!(lru_queue.get(&2), Some(&20));
        assert_eq!(lru_queue.get(&3), Some(&30));
        lru_queue.clear();
        assert_eq!(lru_queue.get(&1), None);
        assert_eq!(lru_queue.get(&2), None);
        assert_eq!(lru_queue.get(&3), None);
        assert_eq!(lru_queue.len(), 0);
    }

    #[test]
    fn test_pop() {
        let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();

        // empty queue
        assert_eq!(lru_queue.pop(), None);

        // simplest case
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        assert_eq!(lru_queue.pop(), Some((1, 10)));
        assert_eq!(lru_queue.pop(), Some((2, 20)));
        assert_eq!(lru_queue.pop(), Some((3, 30)));
        assert_eq!(lru_queue.pop(), None);

        // 'get' changes the order
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        lru_queue.get(&2);
        assert_eq!(lru_queue.pop(), Some((1, 10)));
        assert_eq!(lru_queue.pop(), Some((3, 30)));
        assert_eq!(lru_queue.pop(), Some((2, 20)));
        assert_eq!(lru_queue.pop(), None);

        // multiple 'gets'
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        lru_queue.get(&2);
        lru_queue.get(&3);
        lru_queue.get(&1);
        assert_eq!(lru_queue.pop(), Some((2, 20)));
        assert_eq!(lru_queue.pop(), Some((3, 30)));
        assert_eq!(lru_queue.pop(), Some((1, 10)));
        assert_eq!(lru_queue.pop(), None);

        // 'peak' does not change the order
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        lru_queue.peek(&2);
        assert_eq!(lru_queue.pop(), Some((1, 10)));
        assert_eq!(lru_queue.pop(), Some((2, 20)));
        assert_eq!(lru_queue.pop(), Some((3, 30)));
        assert_eq!(lru_queue.pop(), None);

        // 'contains' does not change the order
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        lru_queue.contains_key(&2);
        assert_eq!(lru_queue.pop(), Some((1, 10)));
        assert_eq!(lru_queue.pop(), Some((2, 20)));
        assert_eq!(lru_queue.pop(), Some((3, 30)));
        assert_eq!(lru_queue.pop(), None);

        // 'put' on the same key promotes it
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        lru_queue.put(2, 21);
        assert_eq!(lru_queue.pop(), Some((1, 10)));
        assert_eq!(lru_queue.pop(), Some((3, 30)));
        assert_eq!(lru_queue.pop(), Some((2, 21)));
        assert_eq!(lru_queue.pop(), None);

        // multiple 'puts'
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        lru_queue.put(2, 21);
        lru_queue.put(3, 31);
        lru_queue.put(1, 11);
        assert_eq!(lru_queue.pop(), Some((2, 21)));
        assert_eq!(lru_queue.pop(), Some((3, 31)));
        assert_eq!(lru_queue.pop(), Some((1, 11)));
        assert_eq!(lru_queue.pop(), None);

        // 'remove' an element in the middle of the queue
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        lru_queue.remove(&2);
        assert_eq!(lru_queue.pop(), Some((1, 10)));
        assert_eq!(lru_queue.pop(), Some((3, 30)));
        assert_eq!(lru_queue.pop(), None);

        // 'remove' the LRU
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        lru_queue.remove(&1);
        assert_eq!(lru_queue.pop(), Some((2, 20)));
        assert_eq!(lru_queue.pop(), Some((3, 30)));
        assert_eq!(lru_queue.pop(), None);

        // 'remove' the MRU
        lru_queue.put(1, 10);
        lru_queue.put(2, 20);
        lru_queue.put(3, 30);
        lru_queue.remove(&3);
        assert_eq!(lru_queue.pop(), Some((1, 10)));
        assert_eq!(lru_queue.pop(), Some((2, 20)));
        assert_eq!(lru_queue.pop(), None);
    }

    #[test]
    /// Fuzzy test using an hashmap as the base to check the methods.
    fn test_fuzzy() {
        let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();
        let mut map: HashMap<i32, i32> = HashMap::new();
        let max_keys = 1_000;
        let methods = ["get", "put", "remove", "pop", "contains", "len"];
        let mut rng = rand::rng();

        for i in 0..1_000_000 {
            match *methods.choose(&mut rng).unwrap() {
                "get" => {
                    assert_eq!(lru_queue.get(&(i % max_keys)), map.get(&(i % max_keys)))
                }
                "put" => assert_eq!(
                    lru_queue.put(i % max_keys, i),
                    map.insert(i % max_keys, i)
                ),
                "remove" => assert_eq!(
                    lru_queue.remove(&(i % max_keys)),
                    map.remove(&(i % max_keys))
                ),
                "pop" => {
                    let removed = lru_queue.pop();
                    if let Some((k, v)) = removed {
                        assert_eq!(Some(v), map.remove(&k))
                    }
                }
                "contains" => {
                    assert_eq!(
                        lru_queue.contains_key(&(i % max_keys)),
                        map.contains_key(&(i % max_keys))
                    )
                }
                "len" => assert_eq!(lru_queue.len(), map.len()),
                _ => unreachable!(),
            }
        }
    }
}
