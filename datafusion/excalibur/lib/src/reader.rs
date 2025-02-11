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

use datafusion_common::Result;

pub trait ExArrayReader<'a> {
    type ValueType;

    // TODO use this for loop unswitching
    /// Returns the length L of the stride of  positions guaranteed to be valid, starting
    /// from the given position S. The position S + L is *not* guaranteed
    /// to be invalid.
    fn valid_stride(&self, start_position: usize) -> usize {
        if self.is_valid(start_position) {
            1
        } else {
            0
        }
    }

    /// Checks whether the position is valid or null.
    ///
    /// Panics if position out of bounds.
    fn is_valid(&self, position: usize) -> bool;

    /// Retrieves the value at the given position.
    ///
    /// Panics if position is invalid or out of bounds.
    fn get(&self, position: usize) -> Self::ValueType;
}

pub trait ExArrayReaderConsumer {
    type ValueType<'a>;

    fn consume<'a, AR>(self, reader: AR) -> Result<()>
    where
        AR: ExArrayReader<'a, ValueType = Self::ValueType<'a>>;
}
