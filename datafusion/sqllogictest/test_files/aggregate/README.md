# Aggregate Tests

##### History and Context:

Aggregate used to be (perhaps still is depending on the progress on issue [#13723](https://github.com/apache/datafusion/issues/13723)).
We have decided to refactor it for better navigation by using `extract and subtract` approach.

Formally, `base_aggregate.slt` starts with all the test cases of original `aggregate.slt` which is currently in `datafusion/sqllogictest/archive`.

Gradually, as we move out(`extract`) different portions of code we remove(`substract`) only that protion from `base_aggregate.slt`.

It should be done in a manner that at all times set of all tests in aggregate folder is a superset of all tests covered in `datafusion/sqllogictest/archive/complete_aggregate.slt`

Refer to [#14301](https://github.com/apache/datafusion/pull/14301) and [#14306](https://github.com/apache/datafusion/pull/14306) for more context and details.
