<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# datafusion-spark: Spark-compatible Expressions

This crate provides Apache Spark-compatible expressions for use with DataFusion.

## Testing Guide

When testing functions by directly invoking them (e.g., `test_scalar_function!()`), input coercion (from the `signature`
or `coerce_types`) is not applied.

Therefore, direct invocation tests should only be used to verify that the function is correctly implemented.

Please be sure to add additional tests beyond direct invocation.
For more detailed testing guidelines, refer to
the [Spark SQLLogicTest README](../sqllogictest/test_files/spark/README.md).

## Implementation References

When implementing Spark-compatible functions, you can check if there are existing implementations in
the [Sail](https://github.com/lakehq/sail) or [Comet](https://github.com/apache/datafusion-comet) projects first.
If you do port functionality from these sources, make sure to port over the corresponding tests too, to ensure
correctness and compatibility.
