# Ballista Examples

This directory contains examples for executing distributed queries with Ballista.

For background information on the Ballista architecture, refer to 
the [Ballista README](../ballista/README.md).

## Start a standalone cluster

From the root of the arrow-datafusion project, build release binaries.

```bash
cargo build --release
```

Start a Ballista scheduler process in a new terminal session.

```bash
RUST_LOG=info ./target/release/ballista-scheduler
```

Start one or more Ballista executor processes in new terminal sessions. When starting more than one 
executor, a unique port number must be specified for each executor.

```bash
RUST_LOG=info ./target/release/ballista-executor -c 4
```

## Running the examples

Refer to the instructions in [DEVELOPERS.md](../DEVELOPERS.md) to define the `ARROW_TEST_DATA` and
`PARQUET_TEST_DATA` environment variables so that the examples can find the test data files.

The examples can be run using the `cargo run --example` syntax. 

```bash
cargo run --release --example ballista-dataframe
```

