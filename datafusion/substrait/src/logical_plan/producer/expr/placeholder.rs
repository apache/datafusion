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

use crate::logical_plan::producer::{SubstraitProducer, to_substrait_type};
use datafusion::common::substrait_err;
use datafusion::logical_expr::expr::Placeholder;
use substrait::proto::expression::RexType;
use substrait::proto::{DynamicParameter, Expression};

pub fn from_placeholder(
    producer: &mut impl SubstraitProducer,
    placeholder: &Placeholder,
) -> datafusion::common::Result<Expression> {
    let parameter_reference = parse_placeholder_index(&placeholder.id)?;

    let r#type = placeholder
        .field
        .as_ref()
        .map(|field| to_substrait_type(producer, field.data_type(), field.is_nullable()))
        .transpose()?;

    Ok(Expression {
        rex_type: Some(RexType::DynamicParameter(DynamicParameter {
            r#type,
            parameter_reference,
        })),
    })
}

/// Converts a placeholder id like "$1" into a zero-based parameter index.
/// Substrait uses zero-based `parameter_reference` while DataFusion uses
/// one-based `$N` placeholder ids.
fn parse_placeholder_index(id: &str) -> datafusion::common::Result<u32> {
    let num_str = id.strip_prefix('$').unwrap_or(id);
    match num_str.parse::<u32>() {
        Ok(n) if n > 0 => Ok(n - 1),
        Ok(_) => substrait_err!("Placeholder index must be >= 1, got: {id}"),
        Err(_) => substrait_err!("Cannot parse placeholder id as numeric index: {id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_placeholder_index() {
        assert_eq!(parse_placeholder_index("$1").unwrap(), 0);
        assert_eq!(parse_placeholder_index("$2").unwrap(), 1);
        assert_eq!(parse_placeholder_index("$100").unwrap(), 99);
        assert!(parse_placeholder_index("$0").is_err());
        assert!(parse_placeholder_index("$name").is_err());
    }
}
