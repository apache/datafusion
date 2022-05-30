#!/usr/bin/env python3
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

import ctypes

datafusion = ctypes.CDLL('libdatafusion_c.so')

datafusion.df_error_free.argtypes = [ctypes.c_void_p]
datafusion.df_error_free.restype = None
datafusion.df_error_get_message.argtypes = [ctypes.c_void_p]
datafusion.df_error_get_message.restype = ctypes.c_char_p

datafusion.df_session_context_new.argtypes = []
datafusion.df_session_context_new.restype = ctypes.c_void_p
datafusion.df_session_context_free.argtypes = [ctypes.c_void_p]
datafusion.df_session_context_free.restype = None
datafusion.df_session_context_sql.argtypes = \
    [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
datafusion.df_session_context_sql.restype = ctypes.c_void_p

datafusion.df_data_frame_free.argtypes = [ctypes.c_void_p]
datafusion.df_data_frame_free.restype = None
datafusion.df_data_frame_show.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
datafusion.df_data_frame_show.restype = None

context = datafusion.df_session_context_new()
try:
    error = (ctypes.c_void_p * 1)()
    try:
        data_frame = datafusion.df_session_context_sql(
            context, b'SELECT 1;', ctypes.pointer(error))
        if error[0] is not None:
            message = datafusion.df_error_get_message(error[0])
            print(f'failed to run SQL: {message.decode()}')
            exit(1)
        try:
            datafusion.df_data_frame_show(data_frame, ctypes.pointer(error));
            if error[0] is not None:
                message = datafusion.df_error_get_message(error[0])
                print('failed to show data frame: {message.decode()}')
                exit(1)
        finally:
            datafusion.df_data_frame_free(data_frame)
    finally:
        if error[0] is not None:
            datafusion.df_error_free(error[0])
finally:
    datafusion.df_session_context_free(context)
