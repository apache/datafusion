# Run db-benchmark

## Directions

Run the following from root `arrow-datafusion` directory

```bash
$ docker build -t db-benchmark -f benchmarks/db-benchmark/db-benchmark.dockerfile .
$ docker run --privileged db-benchmark
```
