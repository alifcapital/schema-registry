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

package io.confluent.connect.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of Converter that uses Avro schemas and objects.
 */
public class AvroConverter implements Converter {

  private SchemaRegistryClient schemaRegistry;
  private Serializer serializer;
  private Deserializer deserializer;

  private boolean isKey;
  private AvroData avroData;
  private AvroSchemaCache avroSchemas;

  public AvroConverter() {
  }

  // Public only for testing
  public AvroConverter(SchemaRegistryClient client) {
    schemaRegistry = client;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    AvroConverterConfig avroConverterConfig = new AvroConverterConfig(configs);

    if (schemaRegistry == null) {
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          avroConverterConfig.getSchemaRegistryUrls(),
          avroConverterConfig.getMaxSchemasPerSubject(),
          Collections.singletonList(new AvroSchemaProvider()),
          configs,
          avroConverterConfig.requestHeaders()
      );
    }

    serializer = new Serializer(configs, schemaRegistry);
    deserializer = new Deserializer(configs, schemaRegistry);
    AvroDataConfig avroDataConfig = new AvroDataConfig(configs);
    avroData = new AvroData(avroDataConfig);
    avroSchemas = new AvroSchemaCache(avroDataConfig.schemaCacheSize());
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    return fromConnectData(topic, null, schema, value);
  }

  @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    try {
      org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
      return serializer.serialize(
          topic,
          isKey,
          headers,
          avroData.fromConnectData(schema, avroSchema, value),
          avroSchemas.get(avroSchema));
    } catch (TimeoutException e) {
      throw new RetriableException(
          String.format("Failed to serialize Avro data from topic %s :", topic),
          e
      );
    } catch (SerializationException e) {
      throw new DataException(
          String.format("Failed to serialize Avro data from topic %s :", topic),
          e
      );
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Avro data from topic %s : %s", topic, e.getMessage())
      );
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic, null, value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    try {
      GenericContainerWithVersion containerWithVersion =
          deserializer.deserialize(topic, isKey, headers, value);
      if (containerWithVersion == null) {
        return SchemaAndValue.NULL;
      }
      GenericContainer deserialized = containerWithVersion.container();
      Integer version = containerWithVersion.version();
      if (deserialized instanceof IndexedRecord) {
        return avroData.toConnectData(deserialized.getSchema(), deserialized, version);
      } else if (deserialized instanceof NonRecordContainer) {
        return avroData.toConnectData(
            deserialized.getSchema(), ((NonRecordContainer) deserialized).getValue(), version);
      }
      throw new DataException(
          String.format("Unsupported type returned during deserialization of topic %s ", topic)
      );
    } catch (TimeoutException e) {
      throw new RetriableException(
          String.format("Failed to deserialize data for topic %s to Avro: ", topic),
          e
      );
    } catch (SerializationException e) {
      throw new DataException(
          String.format("Failed to deserialize data for topic %s to Avro: ", topic),
          e
      );
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Avro data from topic %s : %s", topic, e.getMessage())
      );
    }
  }


  /**
   * Keeps the AvroSchema made for an Avro schema object, and finds it again by identity.
   *
   * <p>AvroData returns the same Avro schema object for every record of a Connect schema. We fear
   * that wrapping it in a new AvroSchema for every record makes the schema registry client compute
   * the hash of a new object and compare it deeply with the cached one each time, so we wrap each
   * Avro schema object once. An Avro schema object that is equal to another one, but not the same
   * object, gets its own AvroSchema, as it would without this cache.
   *
   * <p>We fear that an unbounded cache leaks memory when schemas keep changing, so when it is full
   * we drop everything and start again. The entries are cheap to make.
   */
  static final class AvroSchemaCache {

    private final int maxSize;
    private final ConcurrentHashMap<IdentityKey, AvroSchema> schemas = new ConcurrentHashMap<>();

    AvroSchemaCache(int maxSize) {
      this.maxSize = maxSize;
    }

    AvroSchema get(org.apache.avro.Schema avroSchema) {
      IdentityKey key = new IdentityKey(avroSchema);
      AvroSchema schema = schemas.get(key);
      if (schema == null) {
        if (schemas.size() >= maxSize) {
          schemas.clear();
        }
        schema = new AvroSchema(avroSchema);
        AvroSchema existing = schemas.putIfAbsent(key, schema);
        if (existing != null) {
          schema = existing;
        }
      }
      return schema;
    }

    int size() {
      return schemas.size();
    }
  }

  private static final class IdentityKey {

    private final org.apache.avro.Schema schema;

    IdentityKey(org.apache.avro.Schema schema) {
      this.schema = schema;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(schema);
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof IdentityKey && ((IdentityKey) o).schema == schema;
    }
  }

  private static class Serializer extends AbstractKafkaAvroSerializer {

    public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
      schemaRegistry = client;
      this.autoRegisterSchema = autoRegisterSchema;
    }

    public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {

      this(client, false);
      configure(new KafkaAvroSerializerConfig(configs));
    }

    public byte[] serialize(
        String topic, boolean isKey, Headers headers, Object value, AvroSchema schema) {
      if (value == null) {
        return null;
      }
      return serializeImpl(
          getSubjectName(topic, isKey, value, schema),
          topic,
          headers,
          value,
          schema);
    }
  }

  private static class Deserializer extends AbstractKafkaAvroDeserializer {

    public Deserializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client);
      configure(new KafkaAvroDeserializerConfig(configs));
    }

    public GenericContainerWithVersion deserialize(
        String topic, boolean isKey, Headers headers, byte[] payload) {
      return deserializeWithSchemaAndVersion(topic, isKey, headers, payload);
    }
  }
}
