// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::SchedulerServer;
use ballista_core::{serde::scheduler::ExecutorMeta, BALLISTA_VERSION};
use warp::Rejection;

#[derive(Debug, serde::Serialize)]
struct StateResponse {
    executors: Vec<ExecutorMeta>,
    started: u128,
    version: &'static str,
}

pub(crate) async fn scheduler_state(
    data_server: SchedulerServer,
) -> Result<impl warp::Reply, Rejection> {
    // TODO: Display last seen information in UI
    let executors: Vec<ExecutorMeta> = data_server
        .state
        .get_executors_metadata()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(metadata, _duration)| metadata)
        .collect();
    let response = StateResponse {
        executors,
        started: data_server.start_time,
        version: BALLISTA_VERSION,
    };
    Ok(warp::reply::json(&response))
}
