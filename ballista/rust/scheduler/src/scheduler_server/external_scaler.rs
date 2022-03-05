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

use crate::scheduler_server::externalscaler::{
    external_scaler_server::ExternalScaler, GetMetricSpecResponse, GetMetricsRequest,
    GetMetricsResponse, IsActiveResponse, MetricSpec, MetricValue, ScaledObjectRef,
};
use crate::scheduler_server::SchedulerServer;
use ballista_core::serde::protobuf::task_status;
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan};
use log::debug;
use tonic::{Request, Response};

const INFLIGHT_TASKS_METRIC_NAME: &str = "inflight_tasks";

#[tonic::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> ExternalScaler
    for SchedulerServer<T, U>
{
    async fn is_active(
        &self,
        _request: Request<ScaledObjectRef>,
    ) -> Result<Response<IsActiveResponse>, tonic::Status> {
        let tasks = self.state.get_all_tasks();
        let result = tasks.iter().any(|task| {
            !matches!(
                task.status,
                Some(task_status::Status::Completed(_))
                    | Some(task_status::Status::Failed(_))
            )
        });
        debug!("Are there active tasks? {}", result);
        Ok(Response::new(IsActiveResponse { result }))
    }

    async fn get_metric_spec(
        &self,
        _request: Request<ScaledObjectRef>,
    ) -> Result<Response<GetMetricSpecResponse>, tonic::Status> {
        Ok(Response::new(GetMetricSpecResponse {
            metric_specs: vec![MetricSpec {
                metric_name: INFLIGHT_TASKS_METRIC_NAME.to_string(),
                target_size: 1,
            }],
        }))
    }

    async fn get_metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, tonic::Status> {
        Ok(Response::new(GetMetricsResponse {
            metric_values: vec![MetricValue {
                metric_name: INFLIGHT_TASKS_METRIC_NAME.to_string(),
                metric_value: 10000000, // A very high number to saturate the HPA
            }],
        }))
    }
}
