// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion::{
    common::{DFSchema, substrait_err},
    prelude::{Expr, lambda},
};
use substrait::proto;

use crate::logical_plan::consumer::SubstraitConsumer;

pub async fn from_lambda(
    consumer: &impl SubstraitConsumer,
    expr: &proto::expression::Lambda,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    let Some(parameters) = expr.parameters.as_ref() else {
        return substrait_err!("Lambda expression without parameters is not allowed");
    };

    let names = consumer.push_lambda_parameters(&parameters.types, input_schema)?;

    let Some(body) = expr.body.as_ref() else {
        return substrait_err!("Lambda expression without body is not allowed");
    };

    let body = consumer.consume_expression(body, input_schema).await?;

    consumer.pop_lambda_parameters();

    Ok(lambda(names, body))
}

#[cfg(test)]
mod tests {
    use datafusion::{
        common::{DFSchema, assert_contains},
        prelude::SessionContext,
    };
    use substrait::proto::{self, Expression, r#type::Struct};

    use crate::{
        extensions::Extensions,
        logical_plan::consumer::{DefaultSubstraitConsumer, from_lambda},
    };

    #[tokio::test]
    async fn test_lambda_without_body() {
        let lambda = proto::expression::Lambda {
            parameters: Some(Struct::default()),
            body: None,
        };

        let extensions = Extensions::default();
        let session_state = SessionContext::new().state();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &session_state);

        let err = from_lambda(&consumer, &lambda, DFSchema::empty_ref())
            .await
            .unwrap_err();

        assert_contains!(
            err.to_string(),
            "Lambda expression without body is not allowed"
        );
    }

    #[tokio::test]
    async fn test_lambda_without_parameters() {
        let lambda = proto::expression::Lambda {
            parameters: None,
            body: Some(Box::new(Expression::default())),
        };

        let extensions = Extensions::default();
        let session_state = SessionContext::new().state();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &session_state);

        let err = from_lambda(&consumer, &lambda, DFSchema::empty_ref())
            .await
            .unwrap_err();

        assert_contains!(
            err.to_string(),
            "Lambda expression without parameters is not allowed"
        );
    }
}
