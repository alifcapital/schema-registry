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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class AvroConverterSchemaCacheTest {

  private static final String TOPIC = "topic";
  private static final String AVRO_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Foo\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}";

  private static class RecordingClient extends MockSchemaRegistryClient {
    final List<ParsedSchema> registered = new ArrayList<>();

    @Override
    public RegisterSchemaResponse registerWithResponse(
        String subject, ParsedSchema schema, boolean normalize, boolean propagateSchemaTags)
        throws RestClientException {
      registered.add(schema);
      return super.registerWithResponse(subject, schema, normalize, propagateSchemaTags);
    }
  }

  private static AvroConverter newConverter(SchemaRegistryClient client, int cacheSize) {
    AvroConverter converter = new AvroConverter(client);
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", "http://fake-url");
    config.put("schemas.cache.config", cacheSize);
    converter.configure(config, false);
    return converter;
  }

  private static Schema structSchema(String name) {
    return SchemaBuilder.struct().name(name).field("a", Schema.INT32_SCHEMA).build();
  }

  private static org.apache.avro.Schema parse() {
    return new org.apache.avro.Schema.Parser().parse(AVRO_SCHEMA);
  }

  @Test
  public void sameConnectSchemaIsRegisteredWithTheSameAvroSchemaObject() {
    RecordingClient client = new RecordingClient();
    AvroConverter converter = newConverter(client, 1000);
    Schema schema = structSchema("Foo");
    Schema other = structSchema("Bar");

    byte[] first = converter.fromConnectData(TOPIC, schema, new Struct(schema).put("a", 1));
    byte[] second = converter.fromConnectData(TOPIC, schema, new Struct(schema).put("a", 1));
    byte[] otherBytes = converter.fromConnectData(TOPIC, other, new Struct(other).put("a", 1));

    assertArrayEquals(first, second);
    assertEquals(3, client.registered.size());
    assertSame(client.registered.get(0), client.registered.get(1));
    assertNotSame(client.registered.get(0), client.registered.get(2));
    assertTrue(otherBytes.length > 0);
  }

  @Test
  public void equalConnectSchemaObjectsGiveEqualResults() {
    RecordingClient client = new RecordingClient();
    AvroConverter converter = newConverter(client, 1000);
    Schema schema = structSchema("Foo");
    Schema equalSchema = structSchema("Foo");

    byte[] first = converter.fromConnectData(TOPIC, schema, new Struct(schema).put("a", 1));
    byte[] second = converter.fromConnectData(
        TOPIC, equalSchema, new Struct(equalSchema).put("a", 1));

    assertArrayEquals(first, second);
    assertEquals(client.registered.get(0), client.registered.get(1));
  }

  @Test
  public void convertingStaysCorrectWhenMoreSchemasThanTheCacheHolds() {
    RecordingClient client = new RecordingClient();
    AvroConverter converter = newConverter(client, 2);
    List<Schema> schemas = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      schemas.add(structSchema("Foo" + i));
    }

    for (int round = 0; round < 4; round++) {
      for (int i = 0; i < schemas.size(); i++) {
        Schema schema = schemas.get(i);
        byte[] bytes = converter.fromConnectData(
            TOPIC, schema, new Struct(schema).put("a", round * 10 + i));
        SchemaAndValue converted = converter.toConnectData(TOPIC, bytes);
        assertEquals(round * 10 + i, ((Struct) converted.value()).get("a"));
        assertEquals("Foo" + i, converted.schema().name());
      }
    }
  }

  @Test
  public void cacheFindsAvroSchemaObjectsByIdentity() {
    AvroConverter.AvroSchemaCache cache = new AvroConverter.AvroSchemaCache(10);
    org.apache.avro.Schema first = parse();
    org.apache.avro.Schema equalToFirst = parse();
    assertEquals(first, equalToFirst);

    AvroSchema firstWrapper = cache.get(first);
    assertSame(first, firstWrapper.rawSchema());
    assertSame(firstWrapper, cache.get(first));

    // an equal schema object that is not the same object is not merged with the first one
    AvroSchema equalWrapper = cache.get(equalToFirst);
    assertNotSame(firstWrapper, equalWrapper);
    assertSame(equalToFirst, equalWrapper.rawSchema());
    assertEquals(firstWrapper, equalWrapper);
    assertEquals(2, cache.size());
  }

  @Test
  public void cacheStaysWithinItsLimit() {
    AvroConverter.AvroSchemaCache cache = new AvroConverter.AvroSchemaCache(3);
    for (int i = 0; i < 100; i++) {
      org.apache.avro.Schema schema = parse();
      assertSame(schema, cache.get(schema).rawSchema());
      assertTrue(cache.size() <= 3);
    }
  }

  @Test
  public void threadsGetTheSameAvroSchemaObject() throws Exception {
    final AvroConverter.AvroSchemaCache cache = new AvroConverter.AvroSchemaCache(10);
    final org.apache.avro.Schema schema = parse();
    final int threads = 8;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<AvroSchema>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(executor.submit(() -> {
          start.await();
          AvroSchema last = null;
          for (int i = 0; i < 10_000; i++) {
            last = cache.get(schema);
          }
          return last;
        }));
      }
      start.countDown();
      Set<AvroSchema> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
      for (Future<AvroSchema> future : futures) {
        distinct.add(future.get(60, TimeUnit.SECONDS));
      }
      assertEquals(1, distinct.size());
    } finally {
      executor.shutdownNow();
    }
  }
}
