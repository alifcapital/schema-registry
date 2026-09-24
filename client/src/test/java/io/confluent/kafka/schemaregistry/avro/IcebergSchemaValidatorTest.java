/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.CompatibilityChecker;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.CompatibilityPolicy;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class IcebergSchemaValidatorTest {
  private AvroSchema row(String fields) {
    return new AvroSchema("{\"type\":\"record\",\"name\":\"Row\",\"fields\":[" + fields + "]}");
  }

  private String field(String name, String type) {
    return "{\"name\":\"" + name + "\",\"type\":" + type + "}";
  }

  private List<String> check(AvroSchema next, AvroSchema... history) {
    return CompatibilityChecker.checker(CompatibilityLevel.ICEBERG)
        .isCompatible(next, Arrays.asList(history));
  }

  @Test
  public void addsAndDropsRequiredFieldsWithoutDefaults() {
    AvroSchema first = row(field("a", "\"int\""));
    AvroSchema added = row(field("a", "\"int\"") + "," + field("b", "\"string\""));
    assertTrue(check(added, first).isEmpty());
    assertTrue(check(row(""), first, added).isEmpty());
    assertFalse(CompatibilityChecker.BACKWARD_CHECKER.isCompatible(added,
        Collections.singletonList(first)).isEmpty());
  }

  @Test
  public void ignoresNullabilityAndDefaults() {
    AvroSchema required = row(field("a", "\"int\""));
    AvroSchema optional = row(field("a", "[\"null\",\"int\"]"));
    assertTrue(check(optional, required).isEmpty());
    assertTrue(check(required, optional).isEmpty());
  }

  @Test
  public void remembersRemovedAndWidenedFields() {
    AvroSchema ints = row(field("a", "\"int\""));
    AvroSchema longs = row(field("a", "\"long\""));
    assertTrue(check(longs, ints, row("")).isEmpty());
    assertTrue(check(ints, ints, longs, row("")).isEmpty());
    List<String> errors = check(row(field("a", "\"string\"")), ints, longs, row(""));
    assertTrue(errors.toString(), errors.get(0).contains("$.a"));
    assertTrue(errors.toString(), errors.get(0).contains("retained long"));
    assertTrue(errors.get(0).contains("oldSchema:"));
    assertTrue(errors.get(0).contains("newSchema:"));
  }

  @Test
  public void rejectsAvroPromotionsThatIcebergDoesNotSupport() {
    for (String[] pair : new String[][] {{"int", "double"}, {"long", "float"},
        {"bytes", "string"}, {"string", "bytes"}}) {
      assertFalse(Arrays.toString(pair), check(row(field("a", "\"" + pair[1] + "\"")),
          row(field("a", "\"" + pair[0] + "\""))).isEmpty());
    }
    assertTrue(check(row(field("a", "\"double\"")), row(field("a", "\"float\""))).isEmpty());
  }

  @Test
  public void checksNestedCollectionsAndRetainsNestedDrops() {
    String oldType = "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Nested\",\"fields\":["
        + field("x", "\"int\"") + "]}}";
    String widened = oldType.replace("\"int\"", "\"long\"");
    assertTrue(check(row(field("a", widened)), row(field("a", oldType))).isEmpty());
    assertFalse(check(row(field("a", oldType.replace("\"int\"", "\"string\""))),
        row(field("a", oldType)), row("")).isEmpty());
    assertTrue(check(row(field("a", "{\"type\":\"map\",\"values\":\"long\"}")),
        row(field("a", "{\"type\":\"map\",\"values\":\"int\"}"))).isEmpty());
  }

  private String decimal(int precision, int scale) {
    return "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":" + precision
        + ",\"scale\":" + scale + "}";
  }

  @Test
  public void decimalScaleIsFixedAndPrecisionCanWiden() {
    assertTrue(check(row(field("d", decimal(20, 2))), row(field("d", decimal(10, 2)))).isEmpty());
    assertTrue(check(row(field("d", decimal(10, 2))), row(field("d", decimal(20, 2)))).isEmpty());
    assertFalse(check(row(field("d", decimal(20, 3))), row(field("d", decimal(10, 2)))).isEmpty());
    assertTrue(check(row(field("d", decimal(64, 2)))).isEmpty());
    assertFalse(check(row(field("d", "\"bytes\"")), row(field("d", decimal(10, 2)))).isEmpty());
  }

  @Test
  public void variableDecimalIsNotFixedDecimalOrAnArbitraryStruct() {
    String variable = "{\"type\":\"record\",\"name\":\"io.debezium.data.VariableScaleDecimal\",\"fields\":["
        + field("scale", "\"int\"") + "," + field("value", "\"bytes\"") + "]}";
    assertTrue(check(row(field("d", variable)), row(field("d", variable))).isEmpty());
    assertFalse(check(row(field("d", decimal(20, 2))), row(field("d", variable))).isEmpty());
    assertFalse(check(row(field("d", variable.replace("\"int\"", "\"long\"")))).isEmpty());
  }

  @Test
  public void temporalSemanticChangesRequireManualHandling() {
    AvroSchema millis = row(field("ts", "{\"type\":\"long\",\"connect.name\":\"io.debezium.time.Timestamp\"}"));
    AvroSchema micros = row(field("ts", "{\"type\":\"long\",\"connect.name\":\"io.debezium.time.MicroTimestamp\"}"));
    assertTrue(check(millis, millis).isEmpty());
    assertFalse(check(micros, millis).isEmpty());
    assertFalse(check(row(field("ts", "\"long\"")), millis).isEmpty());
  }

  @Test
  public void connectEncodedMapKeysCannotChange() {
    String map = "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"io.confluent.connect.avro.MapEntry\",\"fields\":["
        + field("key", "\"int\"") + "," + field("value", "\"int\"") + "]}}";
    assertTrue(check(row(field("m", map))).isEmpty());
    assertFalse(check(row(field("m", map.replace("\"key\",\"type\":\"int\"", "\"key\",\"type\":\"long\""))),
        row(field("m", map))).isEmpty());
  }

  @Test
  public void structMapKeysCannotAddDropOrRelaxFields() {
    String key = "{\"type\":\"record\",\"name\":\"Key\",\"fields\":["
        + field("id", "\"int\"") + "]}";
    String map = "{\"type\":\"array\",\"items\":{\"type\":\"record\","
        + "\"name\":\"io.confluent.connect.avro.MapEntry\",\"fields\":["
        + field("key", key) + "," + field("value", "\"int\"") + "]}}";
    AvroSchema before = row(field("m", map));
    assertFalse(check(row(field("m", map.replace(field("id", "\"int\""), ""))),
        before).isEmpty());
    assertFalse(check(row(field("m", map.replace(field("id", "\"int\""),
        field("id", "[\"null\",\"int\"]")))), before).isEmpty());
    assertFalse(check(row(field("m", map.replace(field("id", "\"int\""),
        field("id", "\"int\"") + "," + field("extra", "\"int\"")))), before).isEmpty());
  }

  @Test
  public void rejectsRecursiveAndAmbiguousUnionSchemas() {
    assertFalse(check(row(field("a", "\"null\""))).isEmpty());
    assertFalse(check(row(field("a", "[\"int\",\"string\"]"))).isEmpty());
    assertFalse(check(row(field("a", "[\"null\",\"Row\"]"))).isEmpty());
  }

  @Test
  public void rejectsOtherFormatsEvenWithoutHistoryAndRegardlessOfLenientPolicy() {
    ParsedSchema json = mock(ParsedSchema.class);
    when(json.schemaType()).thenReturn("JSON");
    assertFalse(CompatibilityChecker.ICEBERG_CHECKER.isCompatible(CompatibilityPolicy.LENIENT,
        json, Collections.emptyList()).isEmpty());
    assertEquals(CompatibilityLevel.ICEBERG, CompatibilityLevel.forName("iceberg"));
  }

  @Test
  public void refusesToGuessAcrossIncompatibleHistoricalTypes() {
    List<String> errors = check(row(""), row(field("x", "\"int\"")), row(field("x", "\"string\"")));
    assertTrue(errors.toString(), errors.get(0).contains("existing history"));
  }
}
