/*
 * Copyright 2025 Confluent Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

/**
 * Converting a struct to a record is done in one pass when the Avro schema has the fields of the
 * struct schema in the same order, and with a GenericRecordBuilder otherwise. These tests check
 * that both ways give what a GenericRecordBuilder gives.
 */
public class AvroDataStructConversionTest {

  private final AvroData avroData = new AvroData(10);

  private static org.apache.avro.Schema parse(String json) {
    return new org.apache.avro.Schema.Parser().parse(json);
  }

  private static final Schema CONNECT_SCHEMA = SchemaBuilder.struct().name("Foo")
      .field("id", Schema.INT64_SCHEMA)
      .field("name", Schema.OPTIONAL_STRING_SCHEMA)
      .field("count", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  private static Struct struct(long id, String name, Integer count) {
    return new Struct(CONNECT_SCHEMA).put("id", id).put("name", name).put("count", count);
  }

  private Object convert(Schema schema, org.apache.avro.Schema avroSchema, Object value) {
    return avroData.fromConnectData(schema, avroSchema, value);
  }

  @Test
  public void sameFieldOrderGivesTheRecordOfTheBuilder() {
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(CONNECT_SCHEMA);
    GenericRecord expected = new GenericRecordBuilder(avroSchema)
        .set("id", 5L).set("name", "x").set("count", 7).build();

    Object actual = convert(CONNECT_SCHEMA, avroSchema, struct(5L, "x", 7));

    assertEquals(expected, actual);
    assertTrue(actual instanceof GenericData.Record);
  }

  @Test
  public void nullsInOptionalFieldsStayNull() {
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(CONNECT_SCHEMA);
    GenericRecord expected = new GenericRecordBuilder(avroSchema)
        .set("id", 5L).set("name", null).set("count", null).build();

    assertEquals(expected, convert(CONNECT_SCHEMA, avroSchema, struct(5L, null, null)));
  }

  @Test
  public void nestedStructsAndOptionalStructs() {
    Schema inner = SchemaBuilder.struct().name("Inner").optional()
        .field("v", Schema.OPTIONAL_STRING_SCHEMA).build();
    Schema outer = SchemaBuilder.struct().name("Outer")
        .field("before", inner).field("after", inner).build();
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(outer);
    org.apache.avro.Schema innerAvro = avroSchema.getField("after").schema().getTypes().get(1);

    Struct value = new Struct(outer).put("after", new Struct(inner).put("v", "z"));
    GenericRecord expected = new GenericRecordBuilder(avroSchema)
        .set("before", null)
        .set("after", new GenericRecordBuilder(innerAvro).set("v", "z").build())
        .build();

    assertEquals(expected, convert(outer, avroSchema, value));
  }

  @Test
  public void differentFieldOrderIsMatchedByName() {
    org.apache.avro.Schema avroSchema = parse("{\"type\":\"record\",\"name\":\"Foo\",\"fields\":["
        + "{\"name\":\"count\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"id\",\"type\":\"long\"},"
        + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
    GenericRecord expected = new GenericRecordBuilder(avroSchema)
        .set("id", 5L).set("name", "x").set("count", 7).build();

    assertEquals(expected, convert(CONNECT_SCHEMA, avroSchema, struct(5L, "x", 7)));
  }

  @Test
  public void extraAvroFieldGetsItsDefault() {
    org.apache.avro.Schema avroSchema = parse("{\"type\":\"record\",\"name\":\"Foo\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"long\"},"
        + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"count\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"extra\",\"type\":\"string\",\"default\":\"dflt\"}]}");
    GenericRecord expected = new GenericRecordBuilder(avroSchema)
        .set("id", 5L).set("name", "x").set("count", 7).build();

    GenericRecord actual = (GenericRecord) convert(CONNECT_SCHEMA, avroSchema, struct(5L, "x", 7));

    assertEquals(expected, actual);
    assertEquals("dflt", actual.get("extra").toString());
  }

  @Test
  public void invalidNamesAreScrubbed() {
    AvroData scrubbing = new AvroData(new AvroDataConfig.Builder()
        .with(AvroDataConfig.SCRUB_INVALID_NAMES_CONFIG, true).build());
    Schema schema = SchemaBuilder.struct().name("Foo")
        .field("a-b", Schema.INT32_SCHEMA).field("c.d", Schema.OPTIONAL_STRING_SCHEMA).build();
    org.apache.avro.Schema avroSchema = scrubbing.fromConnectSchema(schema);
    GenericRecord expected = new GenericRecordBuilder(avroSchema)
        .set("a_b", 3).set("c_d", "q").build();

    Object actual = scrubbing.fromConnectData(
        schema, avroSchema, new Struct(schema).put("a-b", 3).put("c.d", "q"));

    assertEquals(expected, actual);
  }

  @Test
  public void nullForAFieldThatDoesNotAcceptItFailsLikeTheBuilder() {
    // the Connect field is optional but the Avro field is not a union with null
    org.apache.avro.Schema avroSchema = parse("{\"type\":\"record\",\"name\":\"Foo\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"long\"},"
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"count\",\"type\":[\"null\",\"int\"],\"default\":null}]}");
    String expectedMessage;
    try {
      new GenericRecordBuilder(avroSchema).set("name", null);
      fail("the builder should refuse null");
      return;
    } catch (AvroRuntimeException e) {
      expectedMessage = e.getMessage();
    }

    try {
      convert(CONNECT_SCHEMA, avroSchema, struct(5L, null, 7));
      fail("expected an exception");
    } catch (AvroRuntimeException e) {
      assertEquals(expectedMessage, e.getMessage());
      assertEquals(AvroRuntimeException.class, e.getClass());
    }
  }

  @Test
  public void nullForAFieldWithADefaultStaysNull() {
    org.apache.avro.Schema avroSchema = parse("{\"type\":\"record\",\"name\":\"Foo\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"long\"},"
        + "{\"name\":\"name\",\"type\":\"string\",\"default\":\"dflt\"},"
        + "{\"name\":\"count\",\"type\":[\"null\",\"int\"],\"default\":null}]}");
    GenericRecord expected = new GenericRecordBuilder(avroSchema)
        .set("id", 5L).set("name", null).set("count", 7).build();

    GenericRecord actual = (GenericRecord) convert(CONNECT_SCHEMA, avroSchema, struct(5L, null, 7));

    assertEquals(expected, actual);
    assertNull(actual.get("name"));
  }

  @Test
  public void avroSchemaThatIsNotARecordFailsLikeBefore() {
    String expectedMessage;
    try {
      new GenericRecordBuilder(parse("\"string\""));
      fail("expected an exception");
      return;
    } catch (AvroRuntimeException e) {
      expectedMessage = e.getMessage();
    }

    try {
      convert(CONNECT_SCHEMA, parse("\"string\""), struct(5L, "x", 7));
      fail("expected an exception");
    } catch (AvroRuntimeException e) {
      assertEquals(expectedMessage, e.getMessage());
    }
  }

  @Test
  public void ignoreDefaultForNullablesKeepsTheNullsOfTheStruct() {
    AvroData ignoring = new AvroData(new AvroDataConfig.Builder()
        .with(AvroDataConfig.IGNORE_DEFAULT_FOR_NULLABLES_CONFIG, true).build());
    Schema schema = SchemaBuilder.struct().name("Foo")
        .field("name", SchemaBuilder.string().optional().defaultValue("dflt").build()).build();
    org.apache.avro.Schema avroSchema = ignoring.fromConnectSchema(schema);

    GenericRecord actual =
        (GenericRecord) ignoring.fromConnectData(schema, avroSchema, new Struct(schema));

    assertNull(actual.get("name"));
  }
}
