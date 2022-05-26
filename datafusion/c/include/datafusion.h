/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#ifdef  __cplusplus
extern "C" {
#endif


typedef struct DFError_ DFError;
extern void df_error_free(DFError *error);
extern const char *df_error_get_message(DFError *error);


typedef struct DFDataFrame_ DFDataFrame;
extern void df_data_frame_free(DFDataFrame *data_frame);
extern void df_data_frame_show(DFDataFrame *data_frame, DFError **error);


typedef struct DFSessionContext_ DFSessionContext;
extern DFSessionContext *df_session_context_new(void);
extern void df_session_context_free(DFSessionContext *ctx);
extern DFDataFrame *df_session_context_sql(DFSessionContext *ctx,
                                           const char *sql,
                                           DFError **error);

#ifdef __cplusplus
}
#endif
