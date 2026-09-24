/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.avro;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.CompatibilityChecker;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import java.util.Collections;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class IcebergCompatibilityTest {
  private final AvroData avroData = new AvroData(16);

  private AvroSchema row(Schema type) {
    return new AvroSchema(avroData.fromConnectSchema(
        SchemaBuilder.struct().name("Row").field("value", type).build()));
  }

  private boolean compatible(Schema next, Schema previous) {
    return CompatibilityChecker.ICEBERG_CHECKER.isCompatible(row(next),
        Collections.singletonList(row(previous))).isEmpty();
  }

  @Test
  public void defaultDecimalPrecisionAndScale() {
    assertTrue(compatible(Decimal.schema(2), Decimal.builder(2)
        .parameter("connect.decimal.precision", "10").build()));
    assertFalse(compatible(Decimal.schema(3), Decimal.schema(2)));
  }

  @Test
  public void debeziumVariableDecimalRemainsDistinct() {
    Schema variable = SchemaBuilder.struct().name("io.debezium.data.VariableScaleDecimal")
        .field("scale", Schema.INT32_SCHEMA).field("value", Schema.BYTES_SCHEMA).build();
    assertTrue(compatible(variable, variable));
    assertFalse(compatible(Decimal.schema(2), variable));
  }

  @Test
  public void connectMapValuesWidenButKeysDoNot() {
    Schema initial = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build();
    assertTrue(compatible(SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT64_SCHEMA).build(),
        initial));
    assertFalse(compatible(SchemaBuilder.map(Schema.INT64_SCHEMA, Schema.INT32_SCHEMA).build(),
        initial));
  }
}
