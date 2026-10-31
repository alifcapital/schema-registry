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

package io.confluent.kafka.schemaregistry.avro;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class NameAwareGenericDataTest {

  private static final String[] WRITER_FIELDS = {"a", "b", "c", "d"};

  private final GenericData data = new AvroSchemaUtils.NameAwareGenericData(GenericData.get());

  // The lookups as they were done before the position was tried first.
  private static Object referenceGetField(Object record, String name, int position) {
    if (record instanceof GenericRecord && ((GenericRecord) record).hasField(name)) {
      return ((GenericRecord) record).get(name);
    } else {
      return ((IndexedRecord) record).get(position);
    }
  }

  private static void referenceSetField(Object record, String name, int position, Object value) {
    if (record instanceof GenericRecord && ((GenericRecord) record).hasField(name)) {
      ((GenericRecord) record).put(name, value);
    } else {
      ((IndexedRecord) record).put(position, value);
    }
  }

  private static Schema recordSchema(String... fieldNames) {
    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("Foo").fields();
    for (String name : fieldNames) {
      fields = fields.requiredString(name);
    }
    return fields.endRecord();
  }

  private static GenericData.Record record(String... fieldNames) {
    GenericData.Record record = new GenericData.Record(recordSchema(fieldNames));
    for (int i = 0; i < fieldNames.length; i++) {
      record.put(i, "value of " + fieldNames[i]);
    }
    return record;
  }

  private static void permute(List<String> chosen, List<String> rest, List<List<String>> out) {
    if (rest.isEmpty()) {
      out.add(new ArrayList<>(chosen));
      return;
    }
    for (int i = 0; i < rest.size(); i++) {
      List<String> remaining = new ArrayList<>(rest);
      chosen.add(remaining.remove(i));
      permute(chosen, remaining, out);
      chosen.remove(chosen.size() - 1);
    }
  }

  // The field names of records that are written with a schema that has the fields a, b, c, d.
  private static List<String[]> recordFieldNames() {
    List<List<String>> permutations = new ArrayList<>();
    permute(new ArrayList<>(), Arrays.asList(WRITER_FIELDS), permutations);
    List<String[]> result = new ArrayList<>();
    for (List<String> permutation : permutations) {
      result.add(permutation.toArray(new String[0]));
    }
    result.add(new String[] {"a", "b", "x", "y"});
    result.add(new String[] {"x", "y", "z", "w"});
    result.add(new String[] {"b", "a", "y", "x"});
    result.add(new String[] {"b", "c"});
    result.add(new String[] {"a", "b"});
    result.add(new String[] {"b", "a"});
    result.add(new String[] {"a", "b", "c", "d", "e"});
    result.add(new String[] {"e", "d", "c", "b", "a"});
    result.add(new String[] {"e", "a", "b", "c", "d"});
    result.add(new String[] {"A", "B", "C", "D"});
    return result;
  }

  private static Object outcome(Callable<Object> call) {
    try {
      return call.call();
    } catch (Exception e) {
      return e.getClass().getName() + ": " + e.getMessage();
    }
  }

  @Test
  public void getFieldReadsByPositionWhenTheFieldsAreInTheSameOrder() {
    GenericData.Record record = record("a", "b", "c", "d");
    for (int i = 0; i < WRITER_FIELDS.length; i++) {
      assertEquals("value of " + WRITER_FIELDS[i], data.getField(record, WRITER_FIELDS[i], i));
    }
  }

  @Test
  public void getFieldFindsReorderedFieldsByName() {
    GenericData.Record record = record("d", "c", "b", "a");
    for (int i = 0; i < WRITER_FIELDS.length; i++) {
      assertEquals("value of " + WRITER_FIELDS[i], data.getField(record, WRITER_FIELDS[i], i));
    }
  }

  @Test
  public void getFieldFallsBackToPositionForRenamedFields() {
    GenericData.Record record = record("x", "y", "z", "w");
    for (int i = 0; i < WRITER_FIELDS.length; i++) {
      assertEquals(
          "value of " + record.getSchema().getFields().get(i).name(),
          data.getField(record, WRITER_FIELDS[i], i));
    }
  }

  @Test
  public void getFieldMixesNameAndPositionLikeBefore() {
    // a is not in the record, so it is read by position, which is the field b
    GenericData.Record record = record("b", "c");
    assertEquals("value of b", data.getField(record, "a", 0));
    assertEquals("value of b", data.getField(record, "b", 1));
  }

  @Test
  public void getFieldGivesTheSameAsLookupByNameForAnyFieldOrder() {
    for (String[] names : recordFieldNames()) {
      final GenericData.Record record = record(names);
      for (int i = 0; i < WRITER_FIELDS.length; i++) {
        final int position = i;
        final String name = WRITER_FIELDS[i];
        assertEquals(
            Arrays.toString(names) + " " + name,
            outcome(() -> referenceGetField(record, name, position)),
            outcome(() -> data.getField(record, name, position)));
      }
    }
  }

  @Test
  public void setFieldGivesTheSameAsLookupByNameForAnyFieldOrder() {
    for (String[] names : recordFieldNames()) {
      for (int i = 0; i < WRITER_FIELDS.length; i++) {
        final int position = i;
        final String name = WRITER_FIELDS[i];
        final GenericData.Record expected = record(names);
        final GenericData.Record actual = record(names);
        Object expectedOutcome =
            outcome(() -> {
              referenceSetField(expected, name, position, "new");
              return null;
            });
        Object actualOutcome =
            outcome(() -> {
              data.setField(actual, name, position, "new");
              return null;
            });
        assertEquals(Arrays.toString(names) + " " + name, expectedOutcome, actualOutcome);
        assertEquals(Arrays.toString(names) + " " + name, expected, actual);
      }
    }
  }

  // An IndexedRecord that is not a GenericRecord is always accessed by position.
  private static class PositionalRecord implements IndexedRecord {
    private final Schema schema;
    private final Object[] values;

    PositionalRecord(Schema schema, Object... values) {
      this.schema = schema;
      this.values = values;
    }

    @Override
    public void put(int i, Object v) {
      values[i] = v;
    }

    @Override
    public Object get(int i) {
      return values[i];
    }

    @Override
    public Schema getSchema() {
      return schema;
    }
  }

  @Test
  public void indexedRecordThatIsNotAGenericRecordIsAccessedByPosition() {
    PositionalRecord record =
        new PositionalRecord(recordSchema("d", "c", "b", "a"), "1", "2", "3", "4");
    for (int i = 0; i < WRITER_FIELDS.length; i++) {
      assertEquals(String.valueOf(i + 1), data.getField(record, WRITER_FIELDS[i], i));
    }
    data.setField(record, "a", 0, "new");
    assertEquals("new", record.get(0));
    data.setField(record, "d", 3, "newer");
    assertEquals("newer", record.get(3));
  }

  @Test
  public void recordWithFewerFieldsThanTheWriterSchemaFailsAsBefore() {
    GenericData.Record record = record("a", "b");
    assertEquals("value of b", data.getField(record, "b", 1));
    final GenericData.Record shortRecord = record;
    Object expected = outcome(() -> referenceGetField(shortRecord, "d", 3));
    Object actual = outcome(() -> data.getField(shortRecord, "d", 3));
    assertEquals(expected, actual);
  }

  private static byte[] write(GenericData data, Schema writerSchema, Object datum)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<>(writerSchema, data).write(datum, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  @Test
  public void writingARecordWithReorderedFieldsGivesTheBytesOfTheWriterOrder() throws IOException {
    Schema writerSchema = recordSchema("a", "b", "c", "d");
    byte[] expected = write(data, writerSchema, record("a", "b", "c", "d"));

    assertArrayEquals(expected, write(data, writerSchema, record("d", "c", "b", "a")));
    assertArrayEquals(expected, write(data, writerSchema, record("c", "a", "d", "b")));
    assertArrayEquals(expected, write(AvroSchemaUtils.getGenericData(true), writerSchema,
        record("b", "a", "d", "c")));
    assertArrayEquals(expected, write(AvroSchemaUtils.getGenericData(false), writerSchema,
        record("b", "a", "d", "c")));
  }

  @Test
  public void writingARecordWithRenamedFieldsTakesTheValuesByPosition() throws IOException {
    Schema writerSchema = recordSchema("a", "b", "c", "d");
    GenericData.Record renamed = new GenericData.Record(recordSchema("w", "x", "y", "z"));
    for (int i = 0; i < 4; i++) {
      renamed.put(i, "value of " + WRITER_FIELDS[i]);
    }

    assertArrayEquals(write(data, writerSchema, record("a", "b", "c", "d")),
        write(data, writerSchema, renamed));
  }

  // ---- resolveUnion

  private static Schema union(Schema... types) {
    return Schema.createUnion(types);
  }

  private static Schema nullable(Schema schema) {
    return union(Schema.create(Schema.Type.NULL), schema);
  }

  private static Schema nullableLast(Schema schema) {
    return union(schema, Schema.create(Schema.Type.NULL));
  }

  private static List<Schema> unions() {
    Schema record = recordSchema("a");
    Schema enumSchema = Schema.createEnum("Color", null, null, Arrays.asList("RED", "GREEN"));
    Schema fixed = Schema.createFixed("Fixed", null, null, 2);
    Schema timestamp = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema decimal = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuid = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    List<Schema> branches = new ArrayList<>(Arrays.asList(
        Schema.create(Schema.Type.LONG),
        Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.DOUBLE),
        Schema.create(Schema.Type.FLOAT),
        Schema.create(Schema.Type.BYTES),
        record, enumSchema, fixed,
        Schema.createArray(Schema.create(Schema.Type.STRING)),
        Schema.createMap(Schema.create(Schema.Type.STRING)),
        timestamp, date, decimal, uuid));
    List<Schema> unions = new ArrayList<>();
    for (Schema branch : branches) {
      unions.add(nullable(branch));
      unions.add(nullableLast(branch));
    }
    unions.add(union(Schema.create(Schema.Type.NULL)));
    unions.add(union(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.LONG)));
    unions.add(union(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.LONG)));
    unions.add(union(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.INT)));
    unions.add(union(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.BYTES)));
    return unions;
  }

  private static List<Object> datums() {
    Schema record = recordSchema("a");
    Schema enumSchema = Schema.createEnum("Color", null, null, Arrays.asList("RED", "GREEN"));
    Schema fixed = Schema.createFixed("Fixed", null, null, 2);
    return Arrays.asList(
        null,
        JsonProperties.NULL_VALUE,
        1L, 1, 1.5f, 1.5d, true, false, (short) 1, (byte) 1, 'c',
        "text", new Utf8("text"), new StringBuilder("text"),
        ByteBuffer.wrap(new byte[] {1, 2}), new byte[] {1, 2},
        record("a"),
        new GenericData.EnumSymbol(enumSchema, "RED"),
        new GenericData.Fixed(fixed, new byte[] {1, 2}),
        new ArrayList<>(Arrays.asList("x", "y")),
        Collections.singletonMap("k", "v"),
        new BigDecimal("1.25"), Instant.ofEpochMilli(1), UUID.randomUUID(),
        new Object());
  }

  private void checkResolveUnion(GenericData actual, GenericData expected) {
    for (Schema union : unions()) {
      for (final Object datum : datums()) {
        final Schema schema = union;
        assertEquals(
            union + " " + datum,
            outcome(() -> expected.resolveUnion(schema, datum)),
            outcome(() -> actual.resolveUnion(schema, datum)));
      }
    }
  }

  @Test
  public void resolveUnionGivesTheSameAsGenericDataWithoutConversions() {
    checkResolveUnion(data, GenericData.get());
  }

  @Test
  public void resolveUnionGivesTheSameAsGenericDataWithConversions() {
    GenericData expected = new GenericData();
    AvroSchemaUtils.addLogicalTypeConversion(expected);
    checkResolveUnion(AvroSchemaUtils.getGenericData(true), expected);
  }

  @Test
  public void resolveUnionGivesTheSameAsGenericDataWithoutADelegate() {
    checkResolveUnion(new AvroSchemaUtils.NameAwareGenericData(), new GenericData());
  }

  @Test
  public void resolveUnionFindsTheBranchOfAnOptionalField() {
    assertEquals(1, data.resolveUnion(nullable(Schema.create(Schema.Type.LONG)), 5L));
    assertEquals(0, data.resolveUnion(nullable(Schema.create(Schema.Type.LONG)), null));
    assertEquals(0, data.resolveUnion(nullableLast(Schema.create(Schema.Type.STRING)), "s"));
    assertEquals(1, data.resolveUnion(nullableLast(Schema.create(Schema.Type.STRING)), null));
    assertEquals(1, data.resolveUnion(nullable(Schema.create(Schema.Type.BYTES)),
        ByteBuffer.wrap(new byte[0])));
  }

  @Test
  public void resolveUnionStillRejectsADatumOfAnotherType() {
    Schema union = nullable(Schema.create(Schema.Type.LONG));
    try {
      data.resolveUnion(union, "text");
      fail("expected an exception");
    } catch (org.apache.avro.UnresolvedUnionException e) {
      assertEquals("Not in union [\"null\",\"long\"]: text", e.getMessage());
    }
    try {
      data.resolveUnion(union, 5);
      fail("expected an exception");
    } catch (org.apache.avro.UnresolvedUnionException e) {
      assertEquals("Not in union [\"null\",\"long\"]: 5", e.getMessage());
    }
  }

  @Test
  public void resolveUnionStillUsesConversionsForABranchWithALogicalType() {
    GenericData withConversions = AvroSchemaUtils.getGenericData(true);
    Schema timestamp = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    assertEquals(1, withConversions.resolveUnion(nullable(timestamp), Instant.ofEpochMilli(5)));
    assertEquals(1, withConversions.resolveUnion(nullable(timestamp), 5L));
  }
}
