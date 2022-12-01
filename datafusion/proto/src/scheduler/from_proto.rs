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

use chrono::{TimeZone, Utc};
use datafusion::physical_plan::metrics::{
    Count, Gauge, MetricValue, MetricsSet, Time, Timestamp,
};
use datafusion::physical_plan::Metric;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

use datafusion_common::DataFusionError;
use crate::protobuf;
use crate::protobuf::action::ActionType;
use crate::protobuf::{operator_metric, NamedCount, NamedGauge, NamedTime};
use crate::scheduler::{
    Action, ExecutorData, ExecutorMetadata, ExecutorSpecification, PartitionId,
    PartitionLocation, PartitionStats, TaskDefinition,
};

impl TryInto<Action> for protobuf::Action {
    type Error = DataFusionError;

    fn try_into(self) -> Result<Action, Self::Error> {
        match self.action_type {
            Some(ActionType::FetchPartition(fetch)) => Ok(Action::FetchPartition {
                job_id: fetch.job_id,
                stage_id: fetch.stage_id as usize,
                partition_id: fetch.partition_id as usize,
                path: fetch.path,
                host: fetch.host,
                port: fetch.port as u16,
            }),
            _ => Err(DataFusionError::Internal(
                "scheduler::from_proto(Action) invalid or missing action".to_owned(),
            )),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<PartitionId> for protobuf::PartitionId {
    fn into(self) -> PartitionId {
        PartitionId::new(
            &self.job_id,
            self.stage_id as usize,
            self.partition_id as usize,
        )
    }
}

#[allow(clippy::from_over_into)]
impl Into<PartitionStats> for protobuf::PartitionStats {
    fn into(self) -> PartitionStats {
        PartitionStats::new(
            foo(self.num_rows),
            foo(self.num_batches),
            foo(self.num_bytes),
        )
    }
}

fn foo(n: i64) -> Option<u64> {
    if n < 0 {
        None
    } else {
        Some(n as u64)
    }
}

impl TryInto<PartitionLocation> for protobuf::PartitionLocation {
    type Error = DataFusionError;

    fn try_into(self) -> Result<PartitionLocation, Self::Error> {
        Ok(PartitionLocation {
            map_partition_id: self.map_partition_id as usize,
            partition_id: self
                .partition_id
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "partition_id in PartitionLocation is missing.".to_owned(),
                    )
                })?
                .into(),
            executor_meta: self
                .executor_meta
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "executor_meta in PartitionLocation is missing".to_owned(),
                    )
                })?
                .into(),
            partition_stats: self
                .partition_stats
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "partition_stats in PartitionLocation is missing".to_owned(),
                    )
                })?
                .into(),
            path: self.path,
        })
    }
}

impl TryInto<MetricValue> for protobuf::OperatorMetric {
    type Error = DataFusionError;

    fn try_into(self) -> Result<MetricValue, Self::Error> {
        match self.metric {
            Some(operator_metric::Metric::OutputRows(value)) => {
                let count = Count::new();
                count.add(value as usize);
                Ok(MetricValue::OutputRows(count))
            }
            Some(operator_metric::Metric::ElapseTime(value)) => {
                let time = Time::new();
                time.add_duration(Duration::from_nanos(value));
                Ok(MetricValue::ElapsedCompute(time))
            }
            Some(operator_metric::Metric::SpillCount(value)) => {
                let count = Count::new();
                count.add(value as usize);
                Ok(MetricValue::SpillCount(count))
            }
            Some(operator_metric::Metric::SpilledBytes(value)) => {
                let count = Count::new();
                count.add(value as usize);
                Ok(MetricValue::SpilledBytes(count))
            }
            Some(operator_metric::Metric::CurrentMemoryUsage(value)) => {
                let gauge = Gauge::new();
                gauge.add(value as usize);
                Ok(MetricValue::CurrentMemoryUsage(gauge))
            }
            Some(operator_metric::Metric::Count(NamedCount { name, value })) => {
                let count = Count::new();
                count.add(value as usize);
                Ok(MetricValue::Count {
                    name: name.into(),
                    count,
                })
            }
            Some(operator_metric::Metric::Gauge(NamedGauge { name, value })) => {
                let gauge = Gauge::new();
                gauge.add(value as usize);
                Ok(MetricValue::Gauge {
                    name: name.into(),
                    gauge,
                })
            }
            Some(operator_metric::Metric::Time(NamedTime { name, value })) => {
                let time = Time::new();
                time.add_duration(Duration::from_nanos(value));
                Ok(MetricValue::Time {
                    name: name.into(),
                    time,
                })
            }
            Some(operator_metric::Metric::StartTimestamp(value)) => {
                let timestamp = Timestamp::new();
                timestamp.set(Utc.timestamp_nanos(value));
                Ok(MetricValue::StartTimestamp(timestamp))
            }
            Some(operator_metric::Metric::EndTimestamp(value)) => {
                let timestamp = Timestamp::new();
                timestamp.set(Utc.timestamp_nanos(value));
                Ok(MetricValue::EndTimestamp(timestamp))
            }
            None => Err(DataFusionError::Internal(
                "scheduler::from_proto(OperatorMetric) metric is None.".to_owned(),
            )),
        }
    }
}

impl TryInto<MetricsSet> for protobuf::OperatorMetricsSet {
    type Error = DataFusionError;

    fn try_into(self) -> Result<MetricsSet, Self::Error> {
        let mut ms = MetricsSet::new();
        let metrics = self
            .metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        for value in metrics {
            let new_metric = Arc::new(Metric::new(value, None));
            ms.push(new_metric)
        }
        Ok(ms)
    }
}

#[allow(clippy::from_over_into)]
impl Into<ExecutorMetadata> for protobuf::ExecutorMetadata {
    fn into(self) -> ExecutorMetadata {
        ExecutorMetadata {
            id: self.id,
            host: self.host,
            port: self.port as u16,
            grpc_port: self.grpc_port as u16,
            specification: self.specification.unwrap().into(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<ExecutorSpecification> for protobuf::ExecutorSpecification {
    fn into(self) -> ExecutorSpecification {
        let mut ret = ExecutorSpecification { task_slots: 0 };
        for resource in self.resources {
            if let Some(protobuf::executor_resource::Resource::TaskSlots(task_slots)) =
                resource.resource
            {
                ret.task_slots = task_slots
            }
        }
        ret
    }
}

#[allow(clippy::from_over_into)]
impl Into<ExecutorData> for protobuf::ExecutorData {
    fn into(self) -> ExecutorData {
        let mut ret = ExecutorData {
            executor_id: self.executor_id,
            total_task_slots: 0,
            available_task_slots: 0,
        };
        for resource in self.resources {
            if let Some(task_slots) = resource.total {
                if let Some(protobuf::executor_resource::Resource::TaskSlots(
                    task_slots,
                )) = task_slots.resource
                {
                    ret.total_task_slots = task_slots
                }
            };
            if let Some(task_slots) = resource.available {
                if let Some(protobuf::executor_resource::Resource::TaskSlots(
                    task_slots,
                )) = task_slots.resource
                {
                    ret.available_task_slots = task_slots
                }
            };
        }
        ret
    }
}

impl TryInto<TaskDefinition> for protobuf::TaskDefinition {
    type Error = DataFusionError;

    fn try_into(self) -> Result<TaskDefinition, Self::Error> {
        let mut props = HashMap::new();
        for kv_pair in self.props {
            props.insert(kv_pair.key, kv_pair.value);
        }

        Ok(TaskDefinition {
            task_id: self.task_id as usize,
            task_attempt_num: self.task_attempt_num as usize,
            job_id: self.job_id,
            stage_id: self.stage_id as usize,
            stage_attempt_num: self.stage_attempt_num as usize,
            partition_id: self.partition_id as usize,
            plan: self.plan,
            output_partitioning: self.output_partitioning,
            session_id: self.session_id,
            launch_time: self.launch_time,
            props,
        })
    }
}

impl TryInto<Vec<TaskDefinition>> for protobuf::MultiTaskDefinition {
    type Error = DataFusionError;

    fn try_into(self) -> Result<Vec<TaskDefinition>, Self::Error> {
        let mut props = HashMap::new();
        for kv_pair in self.props {
            props.insert(kv_pair.key, kv_pair.value);
        }

        let plan = self.plan;
        let output_partitioning = self.output_partitioning;
        let session_id = self.session_id;
        let job_id = self.job_id;
        let stage_id = self.stage_id as usize;
        let stage_attempt_num = self.stage_attempt_num as usize;
        let launch_time = self.launch_time;
        let task_ids = self.task_ids;

        Ok(task_ids
            .iter()
            .map(|task_id| TaskDefinition {
                task_id: task_id.task_id as usize,
                task_attempt_num: task_id.task_attempt_num as usize,
                job_id: job_id.clone(),
                stage_id,
                stage_attempt_num,
                partition_id: task_id.partition_id as usize,
                plan: plan.clone(),
                output_partitioning: output_partitioning.clone(),
                session_id: session_id.clone(),
                launch_time,
                props: props.clone(),
            })
            .collect())
    }
}
