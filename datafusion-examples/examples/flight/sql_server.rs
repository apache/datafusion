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

//! See `main.rs` for how to run it.

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, CommandGetTables,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, ProstMessageExt,
    SqlInfo,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use dashmap::DashMap;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_examples::utils::{datasets::ExampleDataset, write_csv_to_parquet};
use futures::{Stream, StreamExt, TryStreamExt};
use log::info;
use mimalloc::MiMalloc;
use prost::Message;
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
/// Based heavily on Ballista's implementation: https://github.com/apache/datafusion-ballista/blob/main/ballista/scheduler/src/flight_sql.rs
/// and the example in arrow-rs: https://github.com/apache/arrow-rs/blob/master/arrow-flight/examples/flight_sql_server.rs
pub async fn sql_server() -> Result<(), Box<dyn std::error::Error>> {
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

        // Convert the CSV input into a temporary Parquet directory for querying
        let dataset = ExampleDataset::Cars;
        let parquet_temp = write_csv_to_parquet(&ctx, &dataset.path())
            .await
            .map_err(|e| status!("Error writing csv to parquet", e))?;
        let parquet_path = parquet_temp
            .path_str()
            .map_err(|e| status!("Error getting parquet path", e))?;

        // register parquet file with the execution context
        ctx.register_parquet("cars", parquet_path, ParquetReadOptions::default())
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

    async fn tables(&self, ctx: Arc<SessionContext>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));

        let mut catalogs = vec![];
        let mut schemas = vec![];
        let mut names = vec![];
        let mut types = vec![];
        for catalog in ctx.catalog_names() {
            let catalog_provider = ctx.catalog(&catalog).unwrap();
            for schema in catalog_provider.schema_names() {
                let schema_provider = catalog_provider.schema(&schema).unwrap();
                for table in schema_provider.table_names() {
                    let table_provider =
                        schema_provider.table(&table).await.unwrap().unwrap();
                    catalogs.push(catalog.clone());
                    schemas.push(schema.clone());
                    names.push(table.clone());
                    types.push(table_provider.table_type().to_string())
                }
            }
        }

        RecordBatch::try_new(
            schema,
            [catalogs, schemas, names, types]
                .into_iter()
                .map(|i| Arc::new(StringArray::from(i)) as ArrayRef)
                .collect::<Vec<_>>(),
        )
        .unwrap()
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
        let (schema, batches) = match result.first() {
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
        let schema = match result.first() {
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

        let info = FlightInfo::new()
            // Encode the Arrow schema
            .try_with_schema(&schema)
            .expect("encoding failed")
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: Default::default(),
                path: vec![],
            });
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_tables");
        let ctx = self.get_ctx(&request)?;
        let data = self.tables(ctx).await;
        let schema = data.schema();

        let uuid = Uuid::new_v4().hyphenated().to_string();
        self.results.insert(uuid.clone(), vec![data]);

        let fetch = FetchResults { handle: uuid };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };

        let info = FlightInfo::new()
            // Encode the Arrow schema
            .try_with_schema(&schema)
            .expect("encoding failed")
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: Default::default(),
                path: vec![],
            });
        let resp = Response::new(info);
        Ok(resp)
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

        let arrow_schema = plan.schema().as_arrow();
        let message = SchemaAsIpc::new(arrow_schema, &IpcWriteOptions::default())
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
            info!(
                "do_action_close_prepared_statement: removing plan and results for {handle}"
            );
            let _ = self.remove_plan(handle);
            let _ = self.remove_result(handle);
        }
        Ok(())
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
