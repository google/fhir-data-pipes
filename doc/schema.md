# Parquet (and Avro) Schema for FHIR Resources

In this document we review the mapping (a.k.a. projection) from FHIR resources
to Parquet schema. A few high-level reminders:

- The basic idea for the projection follows the
  [Simplified SQL Projection of FHIR Resources](https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md)
  proposal.
- The projection is done using a forked version of the
  [Bunsen library](https://github.com/google/fhir-data-pipes/tree/master/bunsen).
  The entry point for this conversion logic is
  [AvroConverter](https://github.com/google/fhir-data-pipes/blob/master/bunsen/bunsen-avro/src/main/java/com/cerner/bunsen/avro/AvroConverter.java).
  As the name suggests, the conversion logic is from FHIR
  [StructureDefinition](https://hl7.org/fhir/structuredefinition.html)
  to [Apache Avro](https://avro.apache.org/docs/1.11.1/specification/).
- Conversion from Avro to Parquet is done using the
  [parquet-avro](https://www.javadoc.io/static/org.apache.parquet/parquet-avro/1.14.0/org/apache/parquet/avro/package-summary.html#package.description)
  library.

If you want to look at examples before reading the details, you can check
[Patient_no-extension.schema](doc/Patient_no-extension.schema) for the
projection of base [Patient](https://hl7.org/fhir/patient.html) resource.
[Patient_US-Core.schema](Patient_US-Core.schema) provides an example for
[US Core Patient profile](https://build.fhir.org/ig/HL7/US-Core/StructureDefinition-us-core-patient.html).
To see the intermediate Avro schema for this resource, see
[us-core-patient-schema.json](https://github.com/google/fhir-data-pipes/blob/master/bunsen/bunsen-avro/src/test/resources/r4-us-core-schemas/us-core-patient-schema.json).

## Type mapping rules

**Note:** In the following subsections we cover the rules for mapping a FHIR
type to a Parquet schema. As mentioned above, this involves the intermediate
Avro types which are covered as well. In all cases, the _real_ Avro type
is a [union](https://avro.apache.org/docs/1.11.1/specification/#unions) because
all fields are _nullable_. So when we say the FHIR `code` type is mapped to
Avro `string`, it is really the `["null", "string"]` union type. This is not
reiterated below but that is also the reason all Parquet fields are `optional`.
This is even true for fields whose cardinality is exactly one like
[`Observation.status`](https://hl7.org/fhir/observation-definitions.html#Observation.status).

### Primitive types

The [FHIR primitive types](https://hl7.org/fhir/datatypes.html#primitive) are
mapped according to this table
([code reference](https://github.com/google/fhir-data-pipes/blob/c359a08a1bbf449efa206a52c810749e71f34218/bunsen/bunsen-avro/src/main/java/com/cerner/bunsen/avro/converters/DefinitionToAvroVisitor.java#L131)):

| FHIR type      | Avro type | Parquet type                                                                                  |
|----------------|-----------|-----------------------------------------------------------------------------------------------|
| `base64Binary` | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `boolean`      | `boolean` | `boolean`                                                                                     |
| `canonical`    | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `code`         | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `date`         | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `datetime`     | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `decimal`      | `double`  | `double`<sup>*</sup>                                                                          |
| `id`           | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `instant`      | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `integer`      | `int`     | `int32`                                                                                       |
| `markdown`     | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `oid`          | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `positiveInt`  | `int`     | `int32`                                                                                       |
| `string`       | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `time`         | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `unsignedInt`  | `int`     | `int32`                                                                                       |
| `xhtml`        | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `uri`          | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `url`          | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |
| `uuid`         | `string`  | [`STRING`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string-types) |

<sup>*</sup> The original Bunsen used to use Avro
[decimal](https://avro.apache.org/docs/1.11.1/specification/#decimal) type to
represent FHIR `decimal`. But we changed this because of precision issues as
described in [Issue #156](https://github.com/google/fhir-data-pipes/issues/156).

### Records

A FHIR record type, i.e., a complex type that has one or more fields,
are mapped to an
[Avro record](https://avro.apache.org/docs/1.11.1/specification/#schema-record),
which in turn is mapped to Parquet `group`. FHIR examples include any
[Complex Type](https://hl7.org/fhir/datatypes.html#complex),
[BackboneElement](https://hl7.org/fhir/types.html#BackboneElement),
and [Resource](https://hl7.org/fhir/resource.html).

For example a `period` field with FHIR
[Period](https://hl7.org/fhir/datatypes.html#Period) type is mapped to the
following `group` in Parquet:
```
optional group period {
  optional binary start (STRING);
  optional binary end (STRING);
}
```

### Lists

Many FHIR record types have fields that can be repeated. Each element with
[max cardinality](https://hl7.org/fhir/elementdefinition-definitions.html#ElementDefinition.max)
higher than 1 is mapped to an
[Avro array](https://avro.apache.org/docs/1.11.1/specification/#arrays) which in
turn is mapped to a
[Parquet LIST](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists).
As an example, here is the schema for the `address` field of a
[Patient](https://hl7.org/fhir/patient-definitions.html#Patient.address)
resource:

```
optional group address (LIST) {
  repeated group array {
    optional binary use (STRING);
    optional binary type (STRING);
    optional binary text (STRING);
    optional group line (LIST) {
      repeated binary array (STRING);
    }
    optional binary city (STRING);
    optional binary district (STRING);
    optional binary state (STRING);
    optional binary postalCode (STRING);
    optional binary country (STRING);
    optional group period {
      optional binary start (STRING);
      optional binary end (STRING);
    }
  }
}
```

### Choice types

A FHIR "choice type", i.e., fields ending with `[x]` which can take multiple
types, are modeled as a record. The fields of the record are
named after the possible types. For example,
[`Patient.deceased[x]`](https://hl7.org/fhir/patient-definitions.html#Patient.deceased_x_)
can be a `boolean` or a `dateTime`; hence it is modeled with the following
Parquet schema:

```
optional group deceased {
  optional boolean boolean;
  optional binary dateTime (STRING);
}
```

### References

FHIR [references](https://hl7.org/fhir/references.html#Reference) are also
records but because they frequently participate in `JOIN` queries between
different resource tables, they have some extra special fields. These fields
represent each resource type that a reference can refer to and make it easier to
write `JOIN` queries. For example, the
[`Patient.generalPractitioner`](https://hl7.org/fhir/patient-definitions.html#Patient.generalPractitioner)
can be a reference to an `Organization` or `Practitioner` or `PractitionerRole`.
Therefor, it is mapped to the following Parquet schema (only special fields are
shown; note there might be multiple `generalPractitioner`, hence the `LIST`):

```
optional group generalPractitioner (LIST) {
  repeated group array {
    optional binary organizationId (STRING);
    optional binary practitionerId (STRING);
    optional binary practitionerRoleId (STRING);
    ... [rest of the usual fields]
  }
}
```

### Recursion

When mapping FHIR types to Parquet schema, we sometime need to break recursive
structures. For example, a FHIR
[references](https://hl7.org/fhir/references.html#Reference) has an `identifier`
field which has an
[`assigner`](https://hl7.org/fhir/datatypes-definitions.html#Identifier.assigner)
field which is a reference itself. Therefor, there is a
[`recursiveDepth`](https://github.com/google/fhir-data-pipes/blob/6b8cc412331de948eb1fa16d6a84b31a0cea9fc8/pipelines/batch/src/main/java/com/google/fhir/analytics/BasePipelineOptions.java#L64)
configuration parameter that controls how many times a recursive type should
be traversed in the same branch.

### Extensions

To make it easier to query extension fields, top-level fields are created for
them. For example, in the
[US-Core Patient profile](https://hl7.org/fhir/us/core/STU7/StructureDefinition-us-core-patient.html)
there is an extension for
[`birthsex`](https://hl7.org/fhir/us/core/STU7/StructureDefinition-us-core-patient-definitions.html#Patient.extension:birthsex)
whose `type` is `code`; therefor we get the following field at the topmost level
in the Patient Parquet schema:
```
optional binary birthsex (STRING);
```
