#!/usr/bin/env ruby
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require "fiddle/import"

module DataFusion
  extend Fiddle::Importer

  dlload "libdatafusion_c.so"

  typealias "DFError *", "void *"
  extern "void df_error_free(DFError *error)"
  extern "const char *df_error_get_message(DFError *error)"

  typealias "DFDataFrame *", "void *"
  extern "void df_data_frame_free(DFDataFrame *data_frame)"
  extern "void df_data_frame_show(DFDataFrame *data_frame, DFError **error)"

  typealias "DFSessionContext *", "void *"
  extern "DFSessionContext *df_session_context_new(void)"
  extern "void df_session_context_free(DFSessionContext *ctx)"
  extern "DFDataFrame *df_session_context_sql(DFSessionContext *ctx, const char *sql, DFError **error)"
end

begin
  context = DataFusion.df_session_context_new
  Fiddle::Pointer.malloc(Fiddle::SIZEOF_VOIDP, Fiddle::RUBY_FREE) do |error|
    begin
      data_frame = DataFusion.df_session_context_sql(context, "SELECT 1;", error)
      unless error.ptr.null?
        message = DataFusion.df_error_get_message(error.ptr)
        puts("failed to run SQL: #{message}")
        exit(false)
      end
      begin
        DataFusion.df_data_frame_show(data_frame, error)
        unless error.ptr.null?
          message = DataFusion.df_error_get_message(error.ptr)
          puts("failed to show data frame: #{message}")
          exit(false)
        end
      ensure
        DataFusion.df_data_frame_free(data_frame)
      end
    ensure
      DataFusion.df_error_free(error.ptr) unless error.ptr.null?
    end
  end
ensure
  DataFusion.df_session_context_free(context)
end
