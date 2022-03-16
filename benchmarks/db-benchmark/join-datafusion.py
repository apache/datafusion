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

#!/usr/bin/env python

print("# join-datafusion.py", flush=True)

import os
import gc
import timeit
import datafusion as df
from datafusion import functions as f
from datafusion import col
from pyarrow import csv as pacsv

# exec(open("./_helpers/helpers.py").read())

def join_to_tbls(data_name):
    x_n = int(float(data_name.split("_")[1]))
    y_n = ["{:.0e}".format(x_n/1e6), "{:.0e}".format(x_n/1e3), "{:.0e}".format(x_n)]
    y_n = [y_n[0].replace('+0', ''), y_n[1].replace('+0', ''), y_n[2].replace('+0', '')]
    return [data_name.replace('NA', y_n[0]), data_name.replace('NA', y_n[1]), data_name.replace('NA', y_n[2])]


def ans_shape(batches):
    rows, cols = 0, 0
    for batch in batches:
        rows += batch.num_rows
        if cols == 0:
            cols = batch.num_columns
        else:
            assert(cols == batch.num_columns)
    
    return rows, cols

ver = "6.0.0"
task = "join"
git = ""
solution = "datafusion"
fun = ".join"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ["SRC_DATANAME"]
src_jn_x = os.path.join("data", data_name + ".csv")
y_data_name = join_to_tbls(data_name)
src_jn_y = [os.path.join("data", y_data_name[0] + ".csv"), os.path.join("data", y_data_name[1] + ".csv"), os.path.join("data", y_data_name[2] + ".csv")]
if len(src_jn_y) != 3:
  raise Exception("Something went wrong in preparing files used for join")

print("loading datasets " + data_name + ", " + y_data_name[0] + ", " + y_data_name[2] + ", " + y_data_name[2], flush=True)

ctx = df.ExecutionContext()

x_data = pacsv.read_csv(src_jn_x, convert_options=pacsv.ConvertOptions(auto_dict_encode=True))
ctx.register_record_batches("x", [x_data.to_batches()])
small_data = pacsv.read_csv(src_jn_y[0], convert_options=pacsv.ConvertOptions(auto_dict_encode=True))
ctx.register_record_batches("small", [small_data.to_batches()])
medium_data = pacsv.read_csv(src_jn_y[1], convert_options=pacsv.ConvertOptions(auto_dict_encode=True))
ctx.register_record_batches("medium", [medium_data.to_batches()])
large_data = pacsv.read_csv(src_jn_y[2], convert_options=pacsv.ConvertOptions(auto_dict_encode=True))
ctx.register_record_batches("large", [large_data.to_batches()])

print(x_data.num_rows, flush=True)
print(small_data.num_rows, flush=True)
print(medium_data.num_rows, flush=True)
print(large_data.num_rows, flush=True)

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int" # q1
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT x.id1, x.id2, x.id3, x.id4 as xid4, small.id4 as smallid4, x.id5, x.id6, x.v1, small.v2 FROM x INNER JOIN small ON x.id1 = small.id1").collect()
# ans = ctx.sql("SELECT * FROM x INNER JOIN small ON x.id1 = small.id1").collect()
# print(set([b.schema for b in ans]))
shape = ans_shape(ans)
# print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q1: {t}")
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
# m = memory_usage()
# write_log(task=task, data=data_name, in_rows=x_data.num_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()

question = "medium inner on int" # q2
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT x.id1 as xid1, medium.id1 as mediumid1, x.id2, x.id3, x.id4 as xid4, medium.id4 as mediumid4, x.id5 as xid5, medium.id5 as mediumid5, x.id6, x.v1, medium.v2 FROM x INNER JOIN medium ON x.id2 = medium.id2").collect()
shape = ans_shape(ans)
# print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q2: {t}")
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
# m = memory_usage()
# write_log(task=task, data=data_name, in_rows=x_data.num_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()

question = "medium outer on int" # q3
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT x.id1 as xid1, medium.id1 as mediumid1, x.id2, x.id3, x.id4 as xid4, medium.id4 as mediumid4, x.id5 as xid5, medium.id5 as mediumid5, x.id6, x.v1, medium.v2 FROM x LEFT JOIN medium ON x.id2 = medium.id2").collect()
shape = ans_shape(ans)
# print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q3: {t}")
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
# m = memory_usage()
# write_log(task=task, data=data_name, in_rows=x_data.num_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()

question = "medium inner on factor" # q4
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT x.id1 as xid1, medium.id1 as mediumid1, x.id2, x.id3, x.id4 as xid4, medium.id4 as mediumid4, x.id5 as xid5, medium.id5 as mediumid5, x.id6, x.v1, medium.v2 FROM x LEFT JOIN medium ON x.id5 = medium.id5").collect()
shape = ans_shape(ans)
# print(shape)
t = timeit.default_timer() - t_start
print(f"q4: {t}")
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
# m = memory_usage()
# write_log(task=task, data=data_name, in_rows=x_data.num_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()

question = "big inner on int" # q5
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT x.id1 as xid1, large.id1 as largeid1, x.id2 as xid2, large.id2 as largeid2, x.id3, x.id4 as xid4, large.id4 as largeid4, x.id5 as xid5, large.id5 as largeid5, x.id6 as xid6, large.id6 as largeid6, x.v1, large.v2 FROM x LEFT JOIN large ON x.id3 = large.id3").collect()
shape = ans_shape(ans)
# print(shape)
t = timeit.default_timer() - t_start
print(f"q5: {t}")
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
# m = memory_usage()
# write_log(task=task, data=data_name, in_rows=x_data.num_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()

print("joining finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
