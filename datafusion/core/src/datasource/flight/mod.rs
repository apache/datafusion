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

//! Generic [FlightTableFactory] that can connect to Arrow Flight services,
//! with a [sql::FlightSqlDriver] provided out-of-the-box.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_flight::error::FlightError;
use arrow_flight::FlightInfo;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;

use datafusion_catalog::{Session, TableProvider, TableProviderFactory};
use datafusion_common::{project_schema, DataFusionError};
use datafusion_expr::{CreateExternalTable, Expr, TableType};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_expr::Partitioning::UnknownPartitioning;
use datafusion_physical_plan::{ExecutionMode, ExecutionPlan, PlanProperties};

use crate::datasource::physical_plan::FlightExec;

pub mod config;
pub mod sql;

/// Generic Arrow Flight data source. Requires a [FlightDriver] that allows implementors
/// to integrate any custom Flight RPC service by producing a [FlightMetadata] for some DDL.
///
/// # Sample usage:
/// ```
/// use std::collections::HashMap;
/// use arrow_flight::{FlightClient, FlightDescriptor};
/// use tonic::transport::Channel;
/// use datafusion::datasource::flight::{FlightMetadata, FlightDriver};
/// use datafusion::prelude::SessionContext;
/// use std::sync::Arc;
/// use datafusion::datasource::flight::FlightTableFactory;
///
/// #[derive(Debug, Clone, Default)]
/// struct CustomFlightDriver {}
/// #[async_trait::async_trait]
/// impl FlightDriver for CustomFlightDriver {
///     async fn metadata(&self, channel: Channel, opts: &HashMap<String, String>)
///             -> arrow_flight::error::Result<FlightMetadata> {
///         let mut client = FlightClient::new(channel);
///         // the `flight.` prefix is an already registered namespace in datafusion-cli
///         let descriptor = FlightDescriptor::new_cmd(opts["flight.command"].clone());
///         let flight_info = client.get_flight_info(descriptor).await?;
///         FlightMetadata::try_from(flight_info)
///     }
/// }
///
/// #[tokio::main]
/// async fn main() -> datafusion_common::Result<()> {
///     let ctx = SessionContext::new();
///     ctx.state_ref().write().table_factories_mut()
///         .insert("CUSTOM_FLIGHT".into(), Arc::new(FlightTableFactory::new(
///             Arc::new(CustomFlightDriver::default())
///         )));
///     let _ = ctx.sql(r#"
///         CREATE EXTERNAL TABLE custom_flight_table STORED AS CUSTOM_FLIGHT
///         LOCATION 'https://custom.flight.rpc'
///         OPTIONS ('flight.command' 'select * from everywhere')
///     "#).await; // will fail as it can't connect to the bogus URL, but we ignore the error
///     Ok(())
/// }
///
/// ```
#[derive(Clone, Debug)]
pub struct FlightTableFactory {
    driver: Arc<dyn FlightDriver>,
}

impl FlightTableFactory {
    /// Create a data source using the provided driver
    pub fn new(driver: Arc<dyn FlightDriver>) -> Self {
        Self { driver }
    }

    /// Convenient way to create a [FlightTable] programatically, as an alternative to DDL.
    pub async fn open_table(
        &self,
        entry_point: impl Into<String>,
        options: HashMap<String, String>,
    ) -> datafusion_common::Result<FlightTable> {
        let origin = entry_point.into();
        let channel = Channel::from_shared(origin.clone())
            .unwrap()
            .connect()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let metadata = self
            .driver
            .metadata(channel.clone(), &options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let logical_schema = metadata.plan_properties.eq_properties.schema().clone();
        Ok(FlightTable {
            driver: self.driver.clone(),
            channel,
            options,
            origin,
            logical_schema,
        })
    }
}

#[async_trait]
impl TableProviderFactory for FlightTableFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion_common::Result<Arc<dyn TableProvider>> {
        let table = self.open_table(&cmd.location, cmd.options.clone()).await?;
        Ok(Arc::new(table))
    }
}

/// Extension point for integrating any Flight RPC service as a [FlightTableFactory].
/// Handles the initial `GetFlightInfo` call and all its prerequisites (such as `Handshake`),
/// to produce a [FlightMetadata].
#[async_trait]
pub trait FlightDriver: Sync + Send + Debug {
    /// Returns a [FlightMetadata] from the specified channel,
    /// according to the provided table options.
    /// The driver must provide at least a [FlightInfo] in order to construct a flight metadata.
    async fn metadata(
        &self,
        channel: Channel,
        options: &HashMap<String, String>,
    ) -> arrow_flight::error::Result<FlightMetadata>;
}

/// The information that a [FlightDriver] must produce
/// in order to register flights as DataFusion tables.
#[derive(Clone, Debug)]
pub struct FlightMetadata {
    /// FlightInfo object produced by the driver
    pub(super) flight_info: Arc<FlightInfo>,
    /// Physical plan properties. Sensible defaults will be used if the
    /// driver doesn't need (or care) to customize the execution plan.
    pub(super) plan_properties: Arc<PlanProperties>,
    /// The gRPC headers to use on the `DoGet` calls
    pub(super) grpc_metadata: Arc<MetadataMap>,
}

impl FlightMetadata {
    /// Provide custom [PlanProperties] to account for service specifics,
    /// such as known partitioning scheme, unbounded execution mode etc.
    pub fn new(info: FlightInfo, props: PlanProperties, grpc: MetadataMap) -> Self {
        Self {
            flight_info: Arc::new(info),
            plan_properties: Arc::new(props),
            grpc_metadata: Arc::new(grpc),
        }
    }

    /// Uses the default [PlanProperties] and infers the schema from the FlightInfo response.
    pub fn try_new(
        info: FlightInfo,
        grpc: MetadataMap,
    ) -> arrow_flight::error::Result<Self> {
        let schema = Arc::new(info.clone().try_decode_schema()?);
        let partitions = info.endpoint.len();
        let props = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            UnknownPartitioning(partitions),
            ExecutionMode::Bounded,
        );
        Ok(Self::new(info, props, grpc))
    }

    fn with_physical_schema(self, schema: SchemaRef) -> Self {
        let eq_props = EquivalenceProperties::new_with_orderings(
            schema,
            &self.plan_properties.eq_properties.oeq_class().orderings[..],
        );
        let pp = PlanProperties::new(
            eq_props,
            self.plan_properties.partitioning.clone(),
            self.plan_properties.execution_mode,
        );
        Self {
            flight_info: self.flight_info,
            plan_properties: Arc::new(pp),
            grpc_metadata: self.grpc_metadata,
        }
    }
}

/// Uses the default [PlanProperties] and no custom gRPC metadata entries
impl TryFrom<FlightInfo> for FlightMetadata {
    type Error = FlightError;

    fn try_from(info: FlightInfo) -> Result<Self, Self::Error> {
        Self::try_new(info, MetadataMap::default())
    }
}

/// Table provider that wraps a specific flight from an Arrow Flight service
pub struct FlightTable {
    driver: Arc<dyn FlightDriver>,
    channel: Channel,
    options: HashMap<String, String>,
    origin: String,
    logical_schema: SchemaRef,
}

#[async_trait]
impl TableProvider for FlightTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.logical_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let schema = project_schema(&self.logical_schema, projection)?;
        let metadata = self
            .driver
            .metadata(self.channel.clone(), &self.options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .with_physical_schema(schema);
        Ok(Arc::new(FlightExec::new(&metadata, &self.origin)))
    }
}
