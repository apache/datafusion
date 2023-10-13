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

use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginSavepointRequest, ActionBeginSavepointResult,
    ActionBeginTransactionRequest, ActionBeginTransactionResult,
    ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest, ActionEndTransactionRequest, Any, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
    CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo,
    CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementSubstraitPlan, CommandStatementUpdate, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use arrow_schema::Schema;
use dashmap::DashMap;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext};
use futures::{Stream, StreamExt, TryStreamExt};
use log::info;
use mimalloc::MiMalloc;
use prost::Message;
use std::pin::Pin;
use std::sync::Arc;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

/// This example shows how to wrap DataFusion with `FlightSqlService` to support connecting
/// to a standalone DataFusion-based server with a JDBC client, using the open source "JDBC Driver
/// for Arrow Flight SQL".
///
/// To install the JDBC driver in DBeaver for example, see these instructions:
/// https://docs.dremio.com/software/client-applications/dbeaver/
/// When configuring the driver, specify property "UseEncryption" = false
///
/// JDBC connection string: "jdbc:arrow-flight-sql://127.0.0.1:50051/"
///
/// Based heavily on Ballista's implementation: https://github.com/apache/arrow-ballista/blob/main/ballista/scheduler/src/flight_sql.rs
/// and the example in arrow-rs: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples/flight_sql_server.rs
///
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let addr = "0.0.0.0:50051".parse()?;
    let service = FlightSqlServiceImpl {
        contexts: Default::default(),
        statements: Default::default(),
        results: Default::default(),
    };
    info!("Listening on {addr:?}");
    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

pub struct FlightSqlServiceImpl {
    contexts: Arc<DashMap<String, Arc<SessionContext>>>,
    statements: Arc<DashMap<String, LogicalPlan>>,
    results: Arc<DashMap<String, Vec<RecordBatch>>>,
}

impl FlightSqlServiceImpl {
    async fn create_ctx(&self) -> Result<String, Status> {
        let uuid = Uuid::new_v4().hyphenated().to_string();
        let session_config = SessionConfig::from_env()
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?
            .with_information_schema(true);
        let ctx = Arc::new(SessionContext::new_with_config(session_config));

        let testdata = datafusion::test_util::parquet_test_data();

        // register parquet file with the execution context
        ctx.register_parquet(
            "alltypes_plain",
            &format!("{testdata}/alltypes_plain.parquet"),
            ParquetReadOptions::default(),
        )
        .await
        .map_err(|e| status!("Error registering table", e))?;

        self.contexts.insert(uuid.clone(), ctx);
        Ok(uuid)
    }

    fn get_ctx<T>(&self, req: &Request<T>) -> Result<Arc<SessionContext>, Status> {
        // get the token from the authorization header on Request
        let auth = req
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::internal("No authorization header!"))?;
        let str = auth
            .to_str()
            .map_err(|e| Status::internal(format!("Error parsing header: {e}")))?;
        let authorization = str.to_string();
        let bearer = "Bearer ";
        if !authorization.starts_with(bearer) {
            Err(Status::internal("Invalid auth header!"))?;
        }
        let auth = authorization[bearer.len()..].to_string();

        if let Some(context) = self.contexts.get(&auth) {
            Ok(context.clone())
        } else {
            Err(Status::internal(format!(
                "Context handle not found: {auth}"
            )))?
        }
    }

    fn get_plan(&self, handle: &str) -> Result<LogicalPlan, Status> {
        if let Some(plan) = self.statements.get(handle) {
            Ok(plan.clone())
        } else {
            Err(Status::internal(format!("Plan handle not found: {handle}")))?
        }
    }

    fn get_result(&self, handle: &str) -> Result<Vec<RecordBatch>, Status> {
        if let Some(result) = self.results.get(handle) {
            Ok(result.clone())
        } else {
            Err(Status::internal(format!(
                "Request handle not found: {handle}"
            )))?
        }
    }

    fn remove_plan(&self, handle: &str) -> Result<(), Status> {
        self.statements.remove(&handle.to_string());
        Ok(())
    }

    fn remove_result(&self, handle: &str) -> Result<(), Status> {
        self.results.remove(&handle.to_string());
        Ok(())
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        info!("do_handshake");
        // no authentication actually takes place here
        // see Ballista implementation for example of basic auth
        // in this case, we simply accept the connection and create a new SessionContext
        // the SessionContext will be re-used within this same connection/session
        let token = self.create_ctx().await?;

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> =
            Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        if !message.is::<FetchResults>() {
            Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )))?
        }

        let fr: FetchResults = message
            .unpack()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .ok_or_else(|| Status::internal("Expected FetchResults but got None!"))?;

        let handle = fr.handle;

        info!("getting results for {handle}");
        let result = self.get_result(&handle)?;
        // if we get an empty result, create an empty schema
        let (schema, batches) = match result.get(0) {
            None => (Arc::new(Schema::empty()), vec![]),
            Some(batch) => (batch.schema(), result.clone()),
        };

        let batch_stream = futures::stream::iter(batches).map(Ok);

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_statement query:\n{}", query.query);

        Err(Status::unimplemented("Implement get_flight_info_statement"))
    }

    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_substrait_plan");
        Err(Status::unimplemented(
            "Implement get_flight_info_substrait_plan",
        ))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_prepared_statement");
        let handle = std::str::from_utf8(&cmd.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse uuid", e))?;

        let ctx = self.get_ctx(&request)?;
        let plan = self.get_plan(handle)?;

        let state = ctx.state();
        let df = DataFrame::new(state, plan);
        let result = df
            .collect()
            .await
            .map_err(|e| status!("Error executing query", e))?;

        // if we get an empty result, create an empty schema
        let schema = match result.get(0) {
            None => Schema::empty(),
            Some(batch) => (*batch.schema()).clone(),
        };

        self.results.insert(handle.to_string(), result);

        // if we had multiple endpoints to connect to, we could use this Location
        // but in the case of standalone DataFusion, we don't
        // let loc = Location {
        //     uri: "grpc+tcp://127.0.0.1:50051".to_string(),
        // };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
        };
        let endpoints = vec![endpoint];

        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };
        // send -1 for total_records and total_bytes instead of iterating over all the
        // batches to get num_rows() and total byte size.
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: -1_i64,
            total_bytes: -1_i64,
            ordered: false,
        };
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_catalogs");
        Err(Status::unimplemented("Implement get_flight_info_catalogs"))
    }

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_schemas");
        Err(Status::unimplemented("Implement get_flight_info_schemas"))
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_tables");
        Err(Status::unimplemented("Implement get_flight_info_tables"))
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_table_types");
        Err(Status::unimplemented(
            "Implement get_flight_info_table_types",
        ))
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_sql_info");
        Err(Status::unimplemented("Implement CommandGetSqlInfo"))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_primary_keys");
        Err(Status::unimplemented(
            "Implement get_flight_info_primary_keys",
        ))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_exported_keys");
        Err(Status::unimplemented(
            "Implement get_flight_info_exported_keys",
        ))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_imported_keys");
        Err(Status::unimplemented(
            "Implement get_flight_info_imported_keys",
        ))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_cross_reference");
        Err(Status::unimplemented(
            "Implement get_flight_info_cross_reference",
        ))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_xdbc_type_info");
        Err(Status::unimplemented(
            "Implement get_flight_info_xdbc_type_info",
        ))
    }

    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_statement");
        Err(Status::unimplemented("Implement do_get_statement"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_prepared_statement");
        Err(Status::unimplemented("Implement do_get_prepared_statement"))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_catalogs");
        Err(Status::unimplemented("Implement do_get_catalogs"))
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_schemas");
        Err(Status::unimplemented("Implement do_get_schemas"))
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_tables");
        Err(Status::unimplemented("Implement do_get_tables"))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_table_types");
        Err(Status::unimplemented("Implement do_get_table_types"))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_sql_info");
        Err(Status::unimplemented("Implement do_get_sql_info"))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_primary_keys");
        Err(Status::unimplemented("Implement do_get_primary_keys"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_exported_keys");
        Err(Status::unimplemented("Implement do_get_exported_keys"))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_imported_keys");
        Err(Status::unimplemented("Implement do_get_imported_keys"))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_cross_reference");
        Err(Status::unimplemented("Implement do_get_cross_reference"))
    }

    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_xdbc_type_info");
        Err(Status::unimplemented("Implement do_get_xdbc_type_info"))
    }

    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_statement_update");
        Err(Status::unimplemented("Implement do_put_statement_update"))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        info!("do_put_prepared_statement_query");
        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_query",
        ))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _handle: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_prepared_statement_update");
        // statements like "CREATE TABLE.." or "SET datafusion.nnn.." call this function
        // and we are required to return some row count here
        Ok(-1)
    }

    async fn do_put_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_prepared_statement_update");
        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_update",
        ))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let user_query = query.query.as_str();
        info!("do_action_create_prepared_statement: {user_query}");

        let ctx = self.get_ctx(&request)?;

        let plan = ctx
            .sql(user_query)
            .await
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;

        // store a copy of the plan,  it will be used for execution
        let plan_uuid = Uuid::new_v4().hyphenated().to_string();
        self.statements.insert(plan_uuid.clone(), plan.clone());

        let plan_schema = plan.schema();

        let arrow_schema = (&**plan_schema).into();
        let message = SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: plan_uuid.into(),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(),
        };
        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        handle: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        let handle = std::str::from_utf8(&handle.prepared_statement_handle);
        if let Ok(handle) = handle {
            info!("do_action_close_prepared_statement: removing plan and results for {handle}");
            let _ = self.remove_plan(handle);
            let _ = self.remove_result(handle);
        }
        Ok(())
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        info!("do_action_create_prepared_substrait_plan");
        Err(Status::unimplemented(
            "Implement do_action_create_prepared_substrait_plan",
        ))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        info!("do_action_begin_transaction");
        Err(Status::unimplemented(
            "Implement do_action_begin_transaction",
        ))
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        info!("do_action_end_transaction");
        Err(Status::unimplemented("Implement do_action_end_transaction"))
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        info!("do_action_begin_savepoint");
        Err(Status::unimplemented("Implement do_action_begin_savepoint"))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        info!("do_action_end_savepoint");
        Err(Status::unimplemented("Implement do_action_end_savepoint"))
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        info!("do_action_cancel_query");
        Err(Status::unimplemented("Implement do_action_cancel_query"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(string, tag = "1")]
    pub handle: ::prost::alloc::string::String,
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        "type.googleapis.com/datafusion.example.com.sql.FetchResults"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}
