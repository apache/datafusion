<!---
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

## DataFusion in Python

This is a Python library that binds to [Apache Arrow](https://arrow.apache.org/) in-memory query engine [DataFusion](https://github.com/apache/arrow-datafusion).

Like pyspark, it allows you to build a plan through SQL or a DataFrame API against in-memory data, parquet or CSV files, run it in a multi-threaded environment, and obtain the result back in Python.

It also allows you to use UDFs and UDAFs for complex operations.

The major advantage of this library over other execution engines is that this library achieves zero-copy between Python and its execution engine: there is no cost in using UDFs, UDAFs, and collecting the results to Python apart from having to lock the GIL when running those operations.

Its query engine, DataFusion, is written in [Rust](https://www.rust-lang.org/), which makes strong assumptions about thread safety and lack of memory leaks.

Technically, zero-copy is achieved via the [c data interface](https://arrow.apache.org/docs/format/CDataInterface.html).

## How to use it

Simple usage:

```python
import datafusion
import pyarrow

# an alias
f = datafusion.functions

# create a context
ctx = datafusion.ExecutionContext()

# create a RecordBatch and a new DataFrame from it
batch = pyarrow.RecordBatch.from_arrays(
    [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
    names=["a", "b"],
)
df = ctx.create_dataframe([[batch]])

# create a new statement
df = df.select(
    f.col("a") + f.col("b"),
    f.col("a") - f.col("b"),
)

# execute and collect the first (and only) batch
result = df.collect()[0]

assert result.column(0) == pyarrow.array([5, 7, 9])
assert result.column(1) == pyarrow.array([-3, -3, -3])
```

### UDFs

```python
def is_null(array: pyarrow.Array) -> pyarrow.Array:
    return array.is_null()

udf = f.udf(is_null, [pyarrow.int64()], pyarrow.bool_())

df = df.select(udf(f.col("a")))
```

### UDAF

```python
import pyarrow
import pyarrow.compute


class Accumulator:
    """
    Interface of a user-defined accumulation.
    """
    def __init__(self):
        self._sum = pyarrow.scalar(0.0)

    def to_scalars(self) -> [pyarrow.Scalar]:
        return [self._sum]

    def update(self, values: pyarrow.Array) -> None:
        # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
        self._sum = pyarrow.scalar(self._sum.as_py() + pyarrow.compute.sum(values).as_py())

    def merge(self, states: pyarrow.Array) -> None:
        # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
        self._sum = pyarrow.scalar(self._sum.as_py() + pyarrow.compute.sum(states).as_py())

    def evaluate(self) -> pyarrow.Scalar:
        return self._sum


df = ...

udaf = f.udaf(Accumulator, pyarrow.float64(), pyarrow.float64(), [pyarrow.float64()])

df = df.aggregate(
    [],
    [udaf(f.col("a"))]
)
```

## How to install (from pip)

```bash
pip install datafusion
# or
python -m pip install datafusion
```

## How to develop

This assumes that you have rust and cargo installed. We use the workflow recommended by [pyo3](https://github.com/PyO3/pyo3) and [maturin](https://github.com/PyO3/maturin).

Bootstrap:

```bash
# fetch this repo
git clone git@github.com:apache/arrow-datafusion.git
# change to python directory
cd arrow-datafusion/python
# prepare development environment (used to build wheel / install in development)
python3 -m venv venv
# activate the venv
source venv/bin/activate
# update pip itself if necessary
python -m pip install -U pip
# if python -V gives python 3.7
python -m pip install -r requirements-37.txt
# if python -V gives python 3.8/3.9/3.10
python -m pip install -r requirements.txt
```

Whenever rust code changes (your changes or via `git pull`):

```bash
# make sure you activate the venv using "source venv/bin/activate" first
maturin develop
python -m pytest
```

## How to update dependencies

To change test dependencies, change the `requirements.in` and run

```bash
# install pip-tools (this can be done only once), also consider running in venv
python -m pip install pip-tools

# change requirements.in and then run
python -m piptools compile --generate-hashes -o requirements-37.txt
# or run this is you are on python 3.8/3.9/3.10
python -m piptools compile --generate-hashes -o requirements.txt
```

To update dependencies, run with `-U`

```bash
python -m piptools compile -U --generate-hashes -o requirements-310.txt
```

More details [here](https://github.com/jazzband/pip-tools)
