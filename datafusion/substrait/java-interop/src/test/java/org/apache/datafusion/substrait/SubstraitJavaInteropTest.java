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

package org.apache.datafusion.substrait;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.AggregateFunction;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURN;
import io.substrait.proto.Type;
import io.substrait.spark.logical.ToLogicalPlan;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;

/**
 * Runs a Substrait plan produced by DataFusion through substrait-java and Spark.
 *
 * <p>A DataFusion to DataFusion round trip cannot check fields the consumer never reads, so it
 * cannot see a wrong {@code AggregationPhase}: the consumer ignores the field and rebuilds a
 * complete aggregation either way. substrait-spark does read it, and maps the default
 * {@code UNSPECIFIED} to Spark's {@code Final}, which makes Spark reject the plan because the
 * inputs are raw rows rather than partial aggregation buffers.
 *
 * <p>The plan comes from {@code tests/cases/java_interop.rs}. Two workarounds are applied here,
 * each for a known open issue, so that this test fails for the reason it is about and not for an
 * unrelated one.
 */
public class SubstraitJavaInteropTest {

  /** The rows of `t` on this side; the plan reads a table named `t` with one i64 column. */
  private static final String TABLE = "CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES (1L), (2L), (3L) AS t(i)";

  /**
   * apache/datafusion#11545: the producer writes {@code extension_urn_reference = u32::MAX} and a
   * bare function name, so no URN resolves. Point each function at the extension that defines it
   * and give it the compound name substrait-java looks up.
   */
  private static final Map<String, String[]> URNS = Map.of(
      "count", new String[] {"extension:io.substrait:functions_aggregate_generic", "count:any"},
      "sum", new String[] {"extension:io.substrait:functions_arithmetic", "sum:i64"},
      "avg", new String[] {"extension:io.substrait:functions_arithmetic", "avg:fp64"});

  /**
   * apache/datafusion#25049: the producer leaves {@code AggregateFunction.output_type} unset and
   * substrait-java rejects the call. Remove this once that fix lands.
   */
  private static final Map<String, Type> OUTPUT_TYPES = Map.of(
      "count", type(Type.I64.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)),
      "sum", type(Type.I64.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE)),
      "avg", type(Type.FP64.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE)));

  private static Type type(Type.I64.Builder i64) {
    return Type.newBuilder().setI64(i64).build();
  }

  private static Type type(Type.FP64.Builder fp64) {
    return Type.newBuilder().setFp64(fp64).build();
  }

  private static Path planPath() {
    String path = System.getProperty("substrait.interop.plan");
    return path != null ? Paths.get(path) : Paths.get("target", "aggregate_plan.bin");
  }

  /** Applies both workarounds and returns the plan substrait-java can read. */
  private static Plan patched(Plan plan) {
    Plan.Builder builder = plan.toBuilder();
    Map<String, Integer> urnAnchors = new java.util.LinkedHashMap<>();
    Map<Integer, Type> typesByAnchor = new java.util.HashMap<>();

    for (int i = 0; i < builder.getExtensionsCount(); i++) {
      SimpleExtensionDeclaration declaration = builder.getExtensions(i);
      if (!declaration.hasExtensionFunction()) {
        continue;
      }
      String name = declaration.getExtensionFunction().getName();
      String[] urn = URNS.get(name);
      assertTrue(urn != null, "plan declares an unexpected function: " + name);
      typesByAnchor.put(declaration.getExtensionFunction().getFunctionAnchor(), OUTPUT_TYPES.get(name));
      int anchor = urnAnchors.computeIfAbsent(urn[0], key -> urnAnchors.size() + 1);
      builder.setExtensions(
          i,
          declaration.toBuilder()
              .setExtensionFunction(
                  declaration.getExtensionFunction().toBuilder()
                      .setExtensionUrnReference(anchor)
                      .setName(urn[1])));
    }
    urnAnchors.forEach(
        (urn, anchor) ->
            builder.addExtensionUrns(
                SimpleExtensionURN.newBuilder().setExtensionUrnAnchor(anchor).setUrn(urn)));
    return (Plan) fillOutputTypes(builder.build(), typesByAnchor);
  }

  /** Sets `output_type` on every aggregate call, for as long as #25049 is open. */
  private static Message fillOutputTypes(Message message, Map<Integer, Type> byAnchor) {
    Message.Builder builder = message.toBuilder();
    for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
      FieldDescriptor descriptor = field.getKey();
      if (descriptor.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
        continue;
      }
      if (descriptor.isRepeated()) {
        builder.clearField(descriptor);
        for (Object element : (List<?>) field.getValue()) {
          builder.addRepeatedField(descriptor, fillOutputTypes((Message) element, byAnchor));
        }
      } else {
        builder.setField(descriptor, fillOutputTypes((Message) field.getValue(), byAnchor));
      }
    }
    if (builder instanceof AggregateFunction.Builder) {
      AggregateFunction.Builder call = (AggregateFunction.Builder) builder;
      Type type = byAnchor.get(call.getFunctionReference());
      if (type != null && !call.hasOutputType()) {
        call.setOutputType(type);
      }
    }
    return builder.build();
  }

  @Test
  public void sparkRunsTheAggregatePlanDataFusionProduced() throws Exception {
    Path plan = planPath();
    assertTrue(
        Files.exists(plan),
        "run `cargo test -p datafusion-substrait --test substrait_integration -- --ignored "
            + "write_java_interop_plan` first; expected " + plan.toAbsolutePath());

    SparkSession spark =
        SparkSession.builder()
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
    try {
      spark.sparkContext().setLogLevel("ERROR");
      spark.sql(TABLE);

      Plan proto = patched(Plan.parseFrom(Files.readAllBytes(plan)));
      LogicalPlan logical = new ToLogicalPlan(spark).convert(new ProtoPlanConverter().from(proto));

      // The phase decides this: INITIAL_TO_RESULT is Spark's Complete, while the
      // UNSPECIFIED this producer used to write becomes Final, which Spark rejects
      // because the inputs are raw rows rather than partial aggregation buffers.
      List<String> modes = aggregateModes(logical);
      assertEquals(
          List.of("Complete", "Complete", "Complete"),
          modes,
          "the plan's aggregation phase should reach Spark as Complete");

      List<Row> rows = Dataset.ofRows(spark, logical).collectAsList();
      assertEquals(1, rows.size(), "expected one row");
      Row row = rows.get(0);
      assertEquals(3L, row.getLong(0), "count(i)");
      assertEquals(6L, row.getLong(1), "sum(i)");
      assertEquals(2.0d, row.getDouble(2), 0.0d, "avg(i)");
    } finally {
      spark.stop();
    }
  }

  /** The Spark aggregate mode of each aggregate expression, in plan order. */
  private static List<String> aggregateModes(LogicalPlan plan) {
    Matcher matcher =
        Pattern.compile("aggregate\\.(Complete|Final|Partial|PartialMerge)\\$").matcher(plan.toJSON());
    List<String> modes = new java.util.ArrayList<>();
    while (matcher.find()) {
      modes.add(matcher.group(1));
    }
    return modes;
  }
}
