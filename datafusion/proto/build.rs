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

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<(), String> {
    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");
    println!("cargo:rerun-if-changed=proto/datafusion.proto");

    build()?;

    Ok(())
}

#[cfg(feature = "json")]
fn build() -> Result<(), String> {
    use std::io::Write;

    let out = std::path::PathBuf::from(
        std::env::var("OUT_DIR").expect("Cannot find OUT_DIR environment vairable"),
    );
    let descriptor_path = out.join("proto_descriptor.bin");

    prost_build::Config::new()
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile_protos(&["proto/datafusion.proto"], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {}", e))?;

    let descriptor_set = std::fs::read(&descriptor_path)
        .expect(&*format!("Cannot read {:?}", &descriptor_path));

    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)
        .expect(&*format!(
            "Cannot register descriptors {:?}",
            &descriptor_set
        ))
        .build(&[".datafusion"])
        .map_err(|e| format!("pbjson compilation failed: {}", e))?;

    // .serde.rs is not a valid package name, so append to datafusion.rs so we can treat it normally
    let proto = std::fs::read_to_string(out.join("datafusion.rs")).unwrap();
    let json = std::fs::read_to_string(out.join("datafusion.serde.rs")).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open("src/generated/datafusion_json.rs")
        .unwrap();
    file.write(proto.as_str().as_ref()).unwrap();
    file.write(json.as_str().as_ref()).unwrap();

    Ok(())
}

#[cfg(not(feature = "json"))]
fn build() -> Result<(), String> {
    prost_build::Config::new()
        .out_dir("src/generated")
        .compile_protos(&["proto/datafusion.proto"], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {}", e))
}
