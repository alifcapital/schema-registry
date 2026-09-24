# ICEBERG compatibility (fork extension)

This fork adds `ICEBERG` for Avro subjects feeding the schema-evolving Iceberg
sink. It is **not Avro reader/writer compatibility**: consumers requiring Avro
BACKWARD/FORWARD guarantees must not rely on this mode. Existing modes are unchanged.

Enable it per subject, without temporarily switching to NONE:

```sh
curl -X PUT http://localhost:8081/config/orders-value \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"compatibility":"ICEBERG"}'
```

The server build is based on upstream `v8.3.2` and retains the fork's old/new
schema error diagnostics. Deploy the fork to every Registry node before enabling
the mode; upstream binaries do not recognize the new enum. Java 17 or newer is
required by this upstream release.

## Rules

| Source change | Result |
| --- | --- |
| Add or drop an ordinary field, including nested fields | Allowed, no Avro default required |
| Required ↔ nullable | Allowed; sink retains optional columns |
| int → long, float → double | Allowed |
| Narrower source type into a previously widened target | Allowed for the same two pairs |
| Decimal precision change, same scale | Allowed; target retains sufficient precision |
| Decimal scale change | Rejected |
| Other incompatible type change, including string ↔ bytes | Rejected with field path and schemas |
| Map key type/structure change | Rejected |
| Time logical-type name change | Rejected conservatively |
| Recursive records or unions with several non-null branches | Rejected |
| JSON Schema or Protobuf | Rejected |

Rules apply recursively to records, arrays and map values, including Connect's
array-of-MapEntry representation. Record names and field defaults do not define
Iceberg column identity. Field names do: an Avro alias is not a table rename.

Fixed decimals follow the sink's decimal(38,S) representation. AvroConverter's
unspecified precision default of 64 is accepted; actual numeric overflow remains
a sink error. No rounding is authorized by this check. Debezium
VariableScaleDecimal remains a distinct logical type; its per-record scale is
not schema evolution. Whether the sink stores it as a struct or opt-in string
is not configured in Registry, and changing that sink option requires migration.

## History and limits

ICEBERG reconstructs a retained schema from all undeleted subject versions, in
order. Dropping a field does not erase its type. Re-adding it with an incompatible
type is rejected even if it is absent from the latest version. The compatibility
REST endpoint also uses full history when a single version or `latest` is
requested. Compatibility groups do not partition this history, and LENIENT does
not weaken these rules.

Existing incompatible history must be reconciled explicitly before using this
mode. Do not delete schema versions to bypass an error: deleted versions are not
part of the check and their columns may still exist in Iceberg. Registration of
an already registered schema retains upstream's idempotent lookup behavior;
Registry does not inspect each Kafka record or retroactively validate cached IDs.

Registry does not contact the Iceberg catalog and does not know whether a
migration was applied manually. It cannot guarantee compatibility with an
arbitrary pre-existing table. The sink remains responsible for actual table
state, identifier fields, equality deletes, append versus CDC behavior, and
stopping schema changes for CDC without a primary key. No per-stream profiles
or sink configuration synchronization are needed in Registry.

## Verification of this upgrade

Verified locally with OpenJDK 21 and an isolated Apache Kafka **3.7.2** KRaft
broker (PLAINTEXT), using the fork's 8.3.2 server and its unmodified upstream
Kafka client dependency:

- Server startup, creation/writes/reads of the `_schemas_iceberg_smoke` topic.
- BACKWARD rejects an added required field; switching directly to ICEBERG permits it.
- ADD, DROP, widening and rejection of incompatible re-addition, including the
  single-version compatibility endpoint and old/new schema diagnostics.
- Rejection of JSON subjects under ICEBERG.
- Server restart restores the mode and complete schema history; checks and writes
  continue working.
- Two Registry instances join the group; registration through the second instance
  works, and it continues accepting writes after the first instance shuts down.

This verifies the broker interaction needed here, not a production deployment
with your authentication/ACL configuration or a formal vendor support guarantee.
No production cluster was used. Kafka does not need to be upgraded for the tested
flow. Migration from an actual production 7.7.2 `_schemas` topic was not exercised.

The `core` and `avro-converter` reactor dependencies were built and installed with
Checkstyle and SpotBugs enabled (SBOM and Javadoc generation skipped). Targeted
tests passed: 13 validator tests, 3 tests using real AvroData conversion, 7 upstream
Avro compatibility tests, 19 upstream AvroConverter tests, and 1 REST integration
test. Representative commands after installing reactor dependencies:

```sh
mvn -pl client -Dtest=IcebergSchemaValidatorTest test
mvn -pl avro-data -Dtest=IcebergCompatibilityTest test
mvn -pl core -Dtest=AvroCompatibilityTest test
mvn -pl avro-converter -Dtest=AvroConverterTest test
mvn -pl core '-Dit.test=RestApiClusterTest#testIcebergRetainsDroppedFieldTypes' \
  -DargLine= failsafe:integration-test failsafe:verify
```
