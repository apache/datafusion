// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::path::Path;

fn main() -> Result<(), String> {
    let proto_dir = Path::new("datafusion/proto-models");
    let proto_path = Path::new("datafusion/proto-models/proto/datafusion.proto");
    let out_dir = Path::new("datafusion/proto-models/src");

    // proto definitions has to be there
    let descriptor_path = proto_dir.join("proto/proto_descriptor.bin");

    prost_build::Config::new()
        .file_descriptor_set_path(&descriptor_path)
        .out_dir(out_dir)
        .compile_well_known_types()
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile_protos(&[proto_path], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    let descriptor_set = std::fs::read(&descriptor_path)
        .unwrap_or_else(|e| panic!("Cannot read {}: {e}", descriptor_path.display()));

    pbjson_build::Builder::new()
        .out_dir(out_dir)
        .register_descriptors(&descriptor_set)
        .unwrap_or_else(|e| panic!("Cannot register descriptors {descriptor_set:?}: {e}"))
        .build(&[".datafusion"])
        .map_err(|e| format!("pbjson compilation failed: {e}"))?;

    let prost = proto_dir.join("src/datafusion.rs");
    let pbjson = proto_dir.join("src/datafusion.serde.rs");
    let common_path = proto_dir.join("src/datafusion_common.rs");
    println!(
        "Copying {} to {}",
        prost.display(),
        proto_dir.join("src/generated/prost.rs").display()
    );
    std::fs::copy(prost, proto_dir.join("src/generated/prost.rs")).unwrap();
    std::fs::copy(pbjson, proto_dir.join("src/generated/pbjson.rs")).unwrap();
    std::fs::copy(
        common_path,
        proto_dir.join("src/generated/datafusion_proto_common.rs"),
    )
    .unwrap();

    Ok(())
}
