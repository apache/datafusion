# DataFusion + Substrait

[Substrait](https://substrait.io/) provides a cross-language serialization format for relational algebra, based on 
protocol buffers.

This repository provides a Substrait producer and consumer for DataFusion:

- The producer converts a DataFusion logical plan into a Substrait protobuf.
- The consumer converts a Substrait protobuf into a DataFusion logical plan.

Potential uses of this crate:

- Replace the current [DataFusion protobuf definition](https://github.com/apache/arrow-datafusion/blob/master/datafusion-proto/proto/datafusion.proto) used in Ballista for passing query plan fragments to executors
- Make it easier to pass query plans over FFI boundaries, such as from Python to Rust
- Allow Apache Calcite query plans to be executed in DataFusion