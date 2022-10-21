use crate::producer;

use datafusion::error::Result;
use datafusion::prelude::*;

use prost::Message;
use substrait::protobuf::Rel;

use std::fs::OpenOptions;
use std::io::{Write, Read};

pub async fn serialize(sql: &str, ctx: &SessionContext, path: &str) -> Result<()> {
    let df = ctx.sql(sql).await?;
    let plan = df.to_logical_plan()?;
    let proto = producer::to_substrait_rel(&plan)?;

    let mut protobuf_out = Vec::<u8>::new();
    proto.encode(&mut protobuf_out).unwrap();
    let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(path)?;
    file.write_all(&protobuf_out)?;
    Ok(())
}

pub async fn deserialize(path: &str) -> Result<Box<Rel>> {
    let mut protobuf_in = Vec::<u8>::new();

    let mut file = OpenOptions::new()
                .read(true)
                .open(path)?;

    file.read_to_end(&mut protobuf_in)?;
    let proto = Message::decode(&*protobuf_in).unwrap();

    Ok(Box::new(proto))
}


