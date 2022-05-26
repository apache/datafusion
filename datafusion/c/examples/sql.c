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

#include <datafusion.h>

#include <stdio.h>
#include <stdlib.h>

int
main(void)
{
  DFSessionContext *context = df_session_context_new();
  DFError *error = NULL;
  DFDataFrame *data_frame = df_session_context_sql(context, "SELECT 1;", &error);
  if (error) {
    printf("failed to run SQL: %s\n", df_error_get_message(error));
    df_error_free(error);
    df_session_context_free(context);
    return EXIT_FAILURE;
  }
  df_data_frame_show(data_frame, &error);
  if (error) {
    printf("failed to show data frame: %s\n", df_error_get_message(error));
    df_error_free(error);
  }
  df_data_frame_free(data_frame);
  df_session_context_free(context);
  return EXIT_SUCCESS;
}
