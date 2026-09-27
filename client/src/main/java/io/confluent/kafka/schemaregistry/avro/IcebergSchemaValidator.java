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

import io.confluent.kafka.schemaregistry.CompatibilityPolicy;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SchemaValidator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.Schema;

/**
 * Iceberg v2 evolution for schemaful CDC, comparing the candidate with the latest
 * schema only. The live Iceberg catalog and older subject versions are not consulted.
 */
public final class IcebergSchemaValidator implements SchemaValidator {

  @Override
  public List<String> validate(CompatibilityPolicy policy, ParsedSchema candidate,
      Iterable<ParsedSchemaHolder> newestFirst) {
    if (!(candidate instanceof AvroSchema)) {
      return Collections.singletonList("ICEBERG compatibility supports AVRO only; received "
          + candidate.schemaType());
    }
    Map<String, FieldType> retained = new LinkedHashMap<>();
    try {
      for (ParsedSchemaHolder holder : newestFirst) {
        try {
          ParsedSchema previous = holder.schema();
          if (!(previous instanceof AvroSchema)) {
            return Collections.singletonList("ICEBERG requires an AVRO previous schema; "
                + "found " + previous.schemaType());
          }
          merge(retained, describe((AvroSchema) previous));
        } finally {
          holder.clear();
        }
        // An incompatible version accepted under NONE becomes the new baseline.
        break;
      }
      merge(retained, describe((AvroSchema) candidate));
      return Collections.emptyList();
    } catch (Incompatible e) {
      return Collections.singletonList(e.getMessage());
    }
  }

  private static Map<String, FieldType> describe(AvroSchema schema) {
    Map<String, FieldType> fields = new LinkedHashMap<>();
    Origin origin = new Origin(schema.version(), schema.canonicalString());
    visit(schema.rawSchema(), "$", null, fields,
        Collections.newSetFromMap(new IdentityHashMap<>()), origin);
    return fields;
  }

  private static Schema nonNull(Schema schema, String path) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }
    List<Schema> branches = new ArrayList<>();
    for (Schema branch : schema.getTypes()) {
      if (branch.getType() != Schema.Type.NULL) {
        branches.add(branch);
      }
    }
    if (branches.size() != 1) {
      throw new Incompatible("ICEBERG at " + path
          + ": only a nullable union with one non-null type is supported");
    }
    return branches.get(0);
  }

  private static void visit(Schema raw, String path, String keyRoot,
      Map<String, FieldType> fields, Set<Schema> active, Origin origin) {
    if (path.equals(keyRoot) && raw.getType() == Schema.Type.UNION
        && raw.getTypes().stream().anyMatch(branch -> branch.getType() == Schema.Type.NULL)) {
      throw new Incompatible("ICEBERG at " + path + ": map keys cannot be nullable");
    }
    Schema schema = nonNull(raw, path);
    if (schema.getType() == Schema.Type.NULL) {
      throw new Incompatible("ICEBERG at " + path + ": null-only types cannot map to Iceberg v2");
    }
    if (!active.add(schema)) {
      throw new Incompatible("ICEBERG at " + path + ": recursive types cannot map to Iceberg");
    }
    try {
      String logical = schema.getProp("logicalType");
      String connect = schema.getProp("connect.name");
      String semantic = Objects.toString(logical, "") + ":" + Objects.toString(connect, "");
      String kind = schema.getType().getName();
      int precision = 0;
      int scale = 0;
      if ("decimal".equals(logical)) {
        precision = numberProperty(schema, "precision", 0);
        scale = numberProperty(schema, "scale", 0);
        if (precision <= 0 || scale < 0 || scale > 38) {
          throw new Incompatible("ICEBERG at " + path + ": invalid or unsupported decimal scale");
        }
        // AvroConverter uses precision 64 as its unspecified-precision default. The sink
        // stores fixed decimals as decimal(38,S) and checks value overflow at write time.
        precision = Math.min(precision, 38);
        kind = "decimal";
      } else if ("io.debezium.data.VariableScaleDecimal".equals(connect)
          || schema.getType() == Schema.Type.RECORD
          && "io.debezium.data.VariableScaleDecimal".equals(schema.getFullName())) {
        validateVariableDecimal(schema, path);
        fields.put(path, new FieldType("variable-decimal", keySemantic(raw, "", keyRoot),
            0, 0, keyRoot, origin));
        return;
      }
      if (schema.getType() == Schema.Type.FIXED && !"decimal".equals(kind)) {
        semantic += ":size=" + schema.getFixedSize();
      }
      // Ordinary record names are transport names, not Iceberg field identities.
      if (schema.getType() == Schema.Type.RECORD && logical == null) {
        semantic = "";
      }
      if (schema.getType() == Schema.Type.ARRAY && isConnectMap(schema.getElementType())) {
        fields.put(path, new FieldType("map", keySemantic(raw, "", keyRoot),
            0, 0, keyRoot, origin));
        Schema entry = schema.getElementType();
        if (entry.getField("key") == null || entry.getField("value") == null) {
          throw new Incompatible("ICEBERG at " + path + ": malformed Connect map");
        }
        visit(entry.getField("key").schema(), path + "{key}",
            keyRoot == null ? path + "{key}" : keyRoot,
            fields, active, origin);
        visit(entry.getField("value").schema(), path + "{value}", keyRoot,
            fields, active, origin);
        return;
      }
      fields.put(path, new FieldType(kind, keySemantic(raw, semantic, keyRoot),
          precision, scale, keyRoot, origin));
      switch (schema.getType()) {
        case RECORD:
          for (Schema.Field field : schema.getFields()) {
            visit(field.schema(), path + "." + field.name(), keyRoot, fields, active, origin);
          }
          break;
        case ARRAY:
          visit(schema.getElementType(), path + "[]", keyRoot, fields, active, origin);
          break;
        case MAP:
          fields.put(path + "{key}", new FieldType("string", ":", 0, 0,
              keyRoot == null ? path + "{key}" : keyRoot, origin));
          visit(schema.getValueType(), path + "{value}", keyRoot, fields, active, origin);
          break;
        default:
          break;
      }
    } finally {
      active.remove(schema);
    }
  }

  private static String keySemantic(Schema raw, String semantic, String keyRoot) {
    return keyRoot != null && raw.getType() == Schema.Type.UNION
        ? semantic + ":nullable" : semantic;
  }

  private static boolean isConnectMap(Schema schema) {
    return schema.getType() == Schema.Type.RECORD
        && ("io.confluent.connect.avro.MapEntry".equals(schema.getFullName())
            || "MapEntry".equals(schema.getProp("connect.internal.type")));
  }

  private static int numberProperty(Schema schema, String name, int fallback) {
    Object value = schema.getObjectProp(name);
    return value instanceof Number ? ((Number) value).intValue() : fallback;
  }

  private static void validateVariableDecimal(Schema schema, String path) {
    if (schema.getType() != Schema.Type.RECORD || schema.getFields().size() != 2
        || schema.getField("scale") == null || schema.getField("value") == null) {
      throw new Incompatible("ICEBERG at " + path + ": malformed VariableScaleDecimal");
    }
    if (nonNull(schema.getField("scale").schema(), path).getType() != Schema.Type.INT
        || nonNull(schema.getField("value").schema(), path).getType() != Schema.Type.BYTES) {
      throw new Incompatible("ICEBERG at " + path + ": malformed VariableScaleDecimal");
    }
  }

  private static void merge(Map<String, FieldType> retained, Map<String, FieldType> incoming) {
    for (Map.Entry<String, FieldType> entry : incoming.entrySet()) {
      String path = entry.getKey();
      FieldType next = entry.getValue();
      FieldType old = retained.get(path);
      if (old == null) {
        if (next.keyRoot != null && retained.containsKey(next.keyRoot)) {
          throw conflict(path, retained.get(next.keyRoot), next,
              "map keys cannot evolve");
        }
        // A whole new map (including its key structure) may be added.
        continue;
      }
      if (old.same(next)) {
        continue;
      }
      if (next.keyRoot != null || old.keyRoot != null) {
        throw conflict(path, old, next, "map keys cannot evolve");
      }
      if (!old.accepts(next) && !next.accepts(old)) {
        throw conflict(path, old, next, "no lossless Iceberg v2 type promotion");
      }
    }
    for (Map.Entry<String, FieldType> entry : retained.entrySet()) {
      FieldType old = entry.getValue();
      if (old.keyRoot != null && incoming.containsKey(old.keyRoot)
          && !incoming.containsKey(entry.getKey())) {
        throw conflict(entry.getKey(), old, incoming.get(old.keyRoot),
            "map key fields cannot be removed");
      }
    }
    for (Map.Entry<String, FieldType> entry : incoming.entrySet()) {
      FieldType old = retained.get(entry.getKey());
      if (old == null || entry.getValue().accepts(old)) {
        retained.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private static Incompatible conflict(String path, FieldType old, FieldType next,
      String reason) {
    return new Incompatible("ICEBERG at " + path + ": retained " + old + ", incoming " + next
        + "; " + reason
        + "; oldSchemaVersion: " + old.origin.version + "; oldSchema: " + old.origin.schema
        + "; newSchema: " + next.origin.schema);
  }

  private static final class Origin {
    private final Integer version;
    private final String schema;

    private Origin(Integer version, String schema) {
      this.version = version;
      this.schema = schema;
    }
  }

  private static final class FieldType {
    private final String kind;
    private final String semantic;
    private final int precision;
    private final int scale;
    private final String keyRoot;
    private final Origin origin;

    private FieldType(String kind, String semantic, int precision, int scale,
        String keyRoot, Origin origin) {
      this.kind = kind;
      this.semantic = semantic;
      this.precision = precision;
      this.scale = scale;
      this.keyRoot = keyRoot;
      this.origin = origin;
    }

    private boolean same(FieldType other) {
      return kind.equals(other.kind) && semantic.equals(other.semantic)
          && precision == other.precision && scale == other.scale;
    }

    private boolean accepts(FieldType other) {
      if (same(other)) {
        return true;
      }
      if (!semantic.equals(other.semantic)) {
        return false;
      }
      if ("decimal".equals(kind) && "decimal".equals(other.kind)) {
        return scale == other.scale && precision >= other.precision;
      }
      if (!":".equals(semantic)) {
        return false;
      }
      return "long".equals(kind) && "int".equals(other.kind)
          || "double".equals(kind) && "float".equals(other.kind);
    }

    @Override
    public String toString() {
      return kind + ("decimal".equals(kind) ? "(" + precision + "," + scale + ")" : "")
          + (semantic.isEmpty() || ":".equals(semantic) ? "" : " [" + semantic + "]");
    }
  }

  private static final class Incompatible extends RuntimeException {
    private Incompatible(String message) {
      super(message);
    }
  }
}
