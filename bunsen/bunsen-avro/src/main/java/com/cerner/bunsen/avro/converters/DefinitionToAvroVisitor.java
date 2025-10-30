package com.cerner.bunsen.avro.converters;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import com.cerner.bunsen.definitions.DefinitionVisitor;
import com.cerner.bunsen.definitions.DefinitionVisitorsUtil;
import com.cerner.bunsen.definitions.EnumConverter;
import com.cerner.bunsen.definitions.FhirConversionSupport;
import com.cerner.bunsen.definitions.HapiChoiceConverter;
import com.cerner.bunsen.definitions.HapiCompositeConverter;
import com.cerner.bunsen.definitions.HapiContainedConverter;
import com.cerner.bunsen.definitions.HapiConverter;
import com.cerner.bunsen.definitions.HapiConverter.HapiFieldSetter;
import com.cerner.bunsen.definitions.HapiConverter.HapiObjectConverter;
import com.cerner.bunsen.definitions.HapiConverter.MultiValueConverter;
import com.cerner.bunsen.definitions.HapiConverterUtil;
import com.cerner.bunsen.definitions.IdConverter;
import com.cerner.bunsen.definitions.LeafExtensionConverter;
import com.cerner.bunsen.definitions.PrimitiveConverter;
import com.cerner.bunsen.definitions.StringConverter;
import com.cerner.bunsen.definitions.StructureField;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.jspecify.annotations.Nullable;

public class DefinitionToAvroVisitor implements DefinitionVisitor<HapiConverter<Schema>> {

  private final FhirConversionSupport fhirSupport;

  private final String basePackage;

  private final Map<String, HapiConverter<Schema>> visitedConverters;

  private final int recursiveDepth;

  private static final HapiConverter<Schema> STRING_CONVERTER =
      new StringConverter<>(Schema.create(Type.STRING));

  private static final HapiConverter<Schema> ID_CONVERTER =
      new IdConverter<>(Schema.create(Type.STRING));

  private static final HapiConverter<Schema> ENUM_CONVERTER =
      new EnumConverter<>(Schema.create(Type.STRING));

  private static final HapiConverter<Schema> DATE_CONVERTER =
      new StringConverter<>(Schema.create(Type.STRING));

  private static final Schema BOOLEAN_SCHEMA = Schema.create(Type.BOOLEAN);

  private static final HapiConverter<Schema> BOOLEAN_CONVERTER =
      new PrimitiveConverter<Schema>("Boolean") {

        @Override
        public Schema getDataType() {
          return BOOLEAN_SCHEMA;
        }
      };

  private static final Schema INTEGER_SCHEMA = Schema.create(Type.INT);

  private static final HapiConverter<Schema> INTEGER_CONVERTER =
      new PrimitiveConverter<Schema>("Integer") {

        @Override
        public Schema getDataType() {
          return INTEGER_SCHEMA;
        }
      };

  private static final Schema DOUBLE_SCHEMA = Schema.create(Type.DOUBLE);

  private static final String ID_TYPE = "id";

  private static final String R4_STRING_TYPE = "http://hl7.org/fhirpath/System.String";

  // We cannot use Avro logical type `decimal` to represent FHIR `decimal` type. The reason is that
  // Avro `decimal` type  has a fixed scale and a maximum precision but with a fixed scale we have
  // no guarantees on the precision of the FHIR `decimal` type. See this for more details:
  // https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/156#issuecomment-880964207
  // On the other hand, when we convert Avro records back to HAPI objects, we need to convert
  // `Double` values to `BigDecimal `because that's the Java type HAPI uses for `decimal` type of
  // FHIR (Double and BigDecimal are not assignable to each other hence explicit conversions).
  // Note with this approach we are loosing some precision!
  private static final HapiConverter<Schema> DOUBLE_CONVERTER =
      new PrimitiveConverter<Schema>("Decimal") {
        @Override
        public void toHapi(Object input, IPrimitiveType primitive) {
          Preconditions.checkArgument((input instanceof BigDecimal) || (input instanceof Double));
          if (input instanceof Double) {
            primitive.setValue(BigDecimal.valueOf((Double) input));
          } else {
            primitive.setValue(input);
          }
        }

        @Override
        protected Object fromHapi(IPrimitiveType primitive) {
          Object value = primitive.getValue();
          Preconditions.checkState(value instanceof BigDecimal);
          return ((BigDecimal) value).doubleValue();
        }

        @Override
        public Schema getDataType() {
          return DOUBLE_SCHEMA;
        }
      };

  // The key set of this map should be exactly the same as StructureDefinitions.PRIMITIVE_TYPES.
  // TODO refactor/consolidate these two.
  @VisibleForTesting
  static final Map<String, HapiConverter<Schema>> TYPE_TO_CONVERTER =
      ImmutableMap.<String, HapiConverter<Schema>>builder()
          .put(ID_TYPE, ID_CONVERTER)
          .put("boolean", BOOLEAN_CONVERTER)
          .put("code", ENUM_CONVERTER)
          .put("markdown", STRING_CONVERTER)
          .put("date", DATE_CONVERTER)
          .put("instant", DATE_CONVERTER)
          .put("dateTime", DATE_CONVERTER)
          .put("time", STRING_CONVERTER)
          .put("string", STRING_CONVERTER)
          .put("oid", STRING_CONVERTER)
          // Note `xhtml` types are currently skipped because of:
          // https://github.com/google/fhir-data-pipes/issues/1014
          .put("xhtml", STRING_CONVERTER)
          .put("decimal", DOUBLE_CONVERTER)
          .put("integer", INTEGER_CONVERTER)
          .put("unsignedInt", INTEGER_CONVERTER)
          .put("positiveInt", INTEGER_CONVERTER)
          .put("base64Binary", STRING_CONVERTER) // FIXME: convert to Base64
          .put("uri", STRING_CONVERTER)
          // These are added for R4 support.
          .put(R4_STRING_TYPE, STRING_CONVERTER)
          .put("http://hl7.org/fhirpath/System.Boolean", BOOLEAN_CONVERTER)
          .put("http://hl7.org/fhirpath/System.Integer", INTEGER_CONVERTER)
          .put("http://hl7.org/fhirpath/System.Long", INTEGER_CONVERTER)
          .put("http://hl7.org/fhirpath/System.Decimal", DOUBLE_CONVERTER)
          .put("http://hl7.org/fhirpath/System.DateTime", DATE_CONVERTER)
          .put("http://hl7.org/fhirpath/System.Time", STRING_CONVERTER)
          .put("canonical", STRING_CONVERTER)
          .put("url", STRING_CONVERTER)
          .put("uuid", STRING_CONVERTER)
          .build();

  private static class CompositeToAvroConverter extends HapiCompositeConverter<Schema> {

    private final GenericData avroData = SpecificData.get();

    CompositeToAvroConverter(
        @Nullable String elementType,
        List<StructureField<HapiConverter<Schema>>> children,
        Schema structType,
        FhirConversionSupport fhirSupport) {
      this(elementType, children, structType, fhirSupport, null);
    }

    CompositeToAvroConverter(
        @Nullable String elementType,
        List<StructureField<HapiConverter<Schema>>> children,
        Schema structType,
        FhirConversionSupport fhirSupport,
        @Nullable String extensionUrl) {

      super(elementType, children, structType, fhirSupport, extensionUrl);
    }

    @Override
    protected Object getChild(Object composite, int index) {

      // This assumes the Avro datum reader has reconciled any differences between the writer schema
      // (used to produce the Avro data) and the reader schema used in the process consuming it.
      // This reconciliation ensures all field indexes match.
      return ((IndexedRecord) composite).get(index);
    }

    @Override
    protected Object createComposite(Object[] children) {

      IndexedRecord record = (IndexedRecord) avroData.newRecord(null, getDataType());

      for (int i = 0; i < children.length; ++i) {

        record.put(i, children[i]);
      }

      return record;
    }

    @Override
    protected boolean isMultiValued(Schema schema) {

      return schema.getType().equals(Type.ARRAY);
    }

    @Override
    public HapiConverter<Schema> merge(HapiConverter<Schema> other) throws ProfileException {
      HapiConverterUtil.validateIfImplementationClassesAreSame(this, other);
      CompositeToAvroConverter otherConverter = (CompositeToAvroConverter) other;
      // For extensions
      if (!(Strings.isNullOrEmpty(this.extensionUrl())
          && Strings.isNullOrEmpty(otherConverter.extensionUrl()))) {
        if (!Objects.equals(this.extensionUrl(), otherConverter.extensionUrl())) {
          throw new ProfileException(
              String.format(
                  "The extension URL must be the same to merge. currentExtensionUrl=%s,"
                      + " otherExtensionUrl=%s",
                  this.extensionUrl(), otherConverter.extensionUrl()));
        }
        // Return one of the converter as the extension URLs are same.
        return this;
      }

      // For non-extensions
      List<StructureField<HapiConverter<Schema>>> currentChildren = this.getChildren();
      List<StructureField<HapiConverter<Schema>>> otherChildren = otherConverter.getChildren();

      List<StructureField<HapiConverter<Schema>>> mergedList = new ArrayList<>();
      Map<String, StructureField> currentElementsMap =
          currentChildren.stream().collect(Collectors.toMap(s -> s.fieldName(), s -> s));
      Map<String, StructureField> otherElementsMap =
          otherChildren.stream().collect(Collectors.toMap(s -> s.fieldName(), s -> s));

      for (StructureField currentField : currentChildren) {
        if (!otherElementsMap.containsKey(currentField.fieldName())) {
          // Add the current element since it's not available in the other converter.
          mergedList.add(currentField);
        } else {
          // Merge the element if the name is same
          StructureField otherField = otherElementsMap.get(currentField.fieldName());

          HapiConverter currentFieldConverter = (HapiConverter) currentField.result();
          HapiConverter otherFieldConverter = (HapiConverter) otherField.result();

          HapiConverter<Schema> mergedResult = currentFieldConverter.merge(otherFieldConverter);
          StructureField structureField =
              new StructureField(
                  currentField.propertyName(),
                  currentField.fieldName(),
                  currentField.extensionUrl(),
                  currentField.isModifier(),
                  currentField.isChoice(),
                  mergedResult);
          mergedList.add(structureField);
        }
      }

      // Add the remaining elements in the other converter
      for (StructureField otherField : otherChildren) {
        if (!currentElementsMap.containsKey(otherField.fieldName())) {
          mergedList.add(otherField);
        }
      }

      Schema schema = this.getDataType();
      return createCompositeConverter(
          this.getElementType(),
          schema.getName(),
          schema.getDoc(),
          schema.getNamespace(),
          mergedList,
          fhirSupport,
          this.extensionUrl());
    }
  }

  private static class HapiChoiceToAvroConverter extends HapiChoiceConverter<Schema> {

    private final GenericData avroData = SpecificData.get();

    HapiChoiceToAvroConverter(
        Map<String, HapiConverter<Schema>> choiceTypes,
        Schema structType,
        FhirConversionSupport fhirSupport) {

      super(choiceTypes, structType, fhirSupport);
    }

    @Override
    protected Object getChild(Object composite, int index) {

      return ((IndexedRecord) composite).get(index);
    }

    @Override
    protected Object createComposite(Object[] children) {

      IndexedRecord record = (IndexedRecord) avroData.newRecord(null, getDataType());

      for (int i = 0; i < children.length; ++i) {

        record.put(i, children[i]);
      }

      return record;
    }

    @Override
    public HapiConverter<Schema> merge(HapiConverter<Schema> other) throws ProfileException {
      HapiConverterUtil.validateIfImplementationClassesAreSame(this, other);
      HapiChoiceToAvroConverter otherConverter = (HapiChoiceToAvroConverter) other;
      Map<String, HapiConverter<Schema>> currentChoiceTypes = this.getChoiceTypes();
      Map<String, HapiConverter<Schema>> otherChoiceTypes = otherConverter.getChoiceTypes();

      // Use a linked hash map to preserve the order of the fields in the merged converter
      Map<String, HapiConverter<Schema>> mergedChoiceTypes = new LinkedHashMap<>();
      for (String key : currentChoiceTypes.keySet()) {
        if (!otherChoiceTypes.containsKey(key)) {
          mergedChoiceTypes.put(key, currentChoiceTypes.get(key));
        } else {
          HapiConverter<Schema> mergedResult =
              currentChoiceTypes.get(key).merge(otherChoiceTypes.get(key));
          mergedChoiceTypes.put(key, mergedResult);
        }
      }

      for (String key : otherChoiceTypes.keySet()) {
        if (!currentChoiceTypes.containsKey(key)) {
          mergedChoiceTypes.put(key, otherChoiceTypes.get(key));
        }
      }

      return createChoiceConverter(
          mergedChoiceTypes, this.getDataType().getNamespace(), fhirSupport);
    }
  }

  private static class HapiContainedToAvroConverter extends HapiContainedConverter<Schema> {

    private final GenericData avroData = SpecificData.get();

    private HapiContainedToAvroConverter(
        Map<String, StructureField<HapiConverter<Schema>>> contained, Schema structType) {

      super(contained, structType);
    }

    @Override
    protected List<ContainerEntry> getContained(Object container) {

      List containedArray = (List) container;

      List<ContainerEntry> containedEntries = new ArrayList<>();

      for (Object arrayItem : containedArray) {

        IndexedRecord resourceContainer = (IndexedRecord) arrayItem;

        // Coalesce to non-null element in each Resource Container. The number of contained fields
        // will be low, so this nested loop has low cost.
        for (int j = 0; j < resourceContainer.getSchema().getFields().size(); j++) {

          if (resourceContainer.get(j) != null) {

            IndexedRecord record = (IndexedRecord) resourceContainer.get(j);
            String recordType = record.getSchema().getName();

            containedEntries.add(new ContainerEntry(recordType, record));

            break;
          }
        }
      }

      return containedEntries;
    }

    @Override
    protected Object createContained(Object[] contained) {

      List<IndexedRecord> containedList = new ArrayList<>();

      Schema containerType = getDataType().getElementType();

      for (Object containedEntry : contained) {

        IndexedRecord containedRecord = (IndexedRecord) avroData.newRecord(null, containerType);
        String recordName = ((IndexedRecord) containedEntry).getSchema().getName();

        List<Field> fields = containerType.getFields();

        int containedPosition = -1;

        // Find index in the Contained Array which should hold the current contained element; the
        // contained elements Schema name should match the field name.
        for (int j = 0; j < fields.size(); j++) {

          if (fields.get(j).name().equals(recordName)) {

            containedPosition = j;

            break;
          }
        }

        if (containedPosition == -1) {
          String fieldNames =
              containerType.getFields().stream().map(Field::name).collect(Collectors.joining(", "));

          throw new IllegalArgumentException(
              "Expected a field to exist in the Contained List"
                  + " having the name of the current record, but found none."
                  + " Contained Field Names: "
                  + fieldNames
                  + ","
                  + " Record Name: "
                  + recordName);
        }

        containedRecord.put(containedPosition, containedEntry);

        containedList.add(containedRecord);
      }

      return new GenericData.Array<>(getDataType(), containedList);
    }

    @Override
    public HapiConverter<Schema> merge(HapiConverter<Schema> other) throws ProfileException {
      throw new ProfileException("Merging of HapiContainedToAvroConverter is not supported");
    }
  }

  private static class MultiValuedToAvroConverter extends HapiConverter<Schema>
      implements MultiValueConverter {

    private static class MultiValuedtoHapiConverter implements HapiFieldSetter {

      private final HapiObjectConverter elementToHapiConverter;

      MultiValuedtoHapiConverter(HapiObjectConverter elementToHapiConverter) {
        this.elementToHapiConverter = elementToHapiConverter;
      }

      @Override
      public void setField(
          IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object element) {

        for (Object value : (Iterable) element) {

          Object hapiObject = elementToHapiConverter.toHapi(value);

          fieldToSet.getMutator().addValue(parentObject, (IBase) hapiObject);
        }
      }
    }

    HapiConverter<Schema> elementConverter;

    MultiValuedToAvroConverter(HapiConverter<Schema> elementConverter) {
      this.elementConverter = elementConverter;
    }

    @Override
    public HapiConverter getElementConverter() {
      return elementConverter;
    }

    @Override
    public Object fromHapi(Object input) {

      List list = (List) input;

      List convertedList =
          (List)
              list.stream()
                  .map(item -> elementConverter.fromHapi(item))
                  .collect(Collectors.toList());

      return new GenericData.Array<>(getDataType(), convertedList);
    }

    @Override
    public Schema getDataType() {

      return Schema.createArray(elementConverter.getDataType());
    }

    @Override
    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      BaseRuntimeElementDefinition elementDefinition = elementDefinitions[0];

      HapiObjectConverter rowToHapiConverter =
          (HapiObjectConverter) elementConverter.toHapiConverter(elementDefinition);

      return new MultiValuedtoHapiConverter(rowToHapiConverter);
    }

    @Override
    public HapiConverter<Schema> merge(HapiConverter<Schema> other) throws ProfileException {
      HapiConverterUtil.validateIfImplementationClassesAreSame(this, other);
      HapiConverter<Schema> mergedElementConverter =
          this.elementConverter.merge(((MultiValuedToAvroConverter) other).getElementConverter());
      return new MultiValuedToAvroConverter(mergedElementConverter);
    }
  }

  /**
   * Creates a visitor to construct Avro conversion objects.
   *
   * @param fhirSupport support for FHIR conversions.
   * @param basePackage the base package to be used for generated Avro structures.
   * @param visitedConverters a mutable cache of generated converters that may be reused by types
   *     that contain them.
   */
  public DefinitionToAvroVisitor(
      FhirConversionSupport fhirSupport,
      String basePackage,
      Map<String, HapiConverter<Schema>> visitedConverters,
      int recursiveDepth) {
    this.fhirSupport = fhirSupport;
    this.basePackage = basePackage;
    this.visitedConverters = visitedConverters;
    this.recursiveDepth = recursiveDepth;
  }

  @Override
  public HapiConverter<Schema> visitPrimitive(String elementName, String primitiveType) {
    // For R4, `id` elements use `System.String` type instead of `id`; that's why we do this check.
    // Note for resource `id`s we should use ID_CONVERTER but for type `id`s use STRING_CONVERTER;
    // this is handled in ID_CONVERTER.
    String realType = primitiveType;
    if (ID_TYPE.equals(elementName) && R4_STRING_TYPE.equals(primitiveType)) {
      realType = ID_TYPE;
    }
    Preconditions.checkNotNull(
        TYPE_TO_CONVERTER.get(realType),
        "No converter found for the primitive type " + primitiveType + " real: " + realType);
    return TYPE_TO_CONVERTER.get(realType);
  }

  private static final Schema NULL_SCHEMA = Schema.create(Type.NULL);

  /** Makes a given schema nullable. */
  private static Schema nullable(Schema schema) {

    return Schema.createUnion(Arrays.asList(NULL_SCHEMA, schema));
  }

  @Override
  public HapiConverter<Schema> visitContained(
      String elementPath,
      String elementTypeUrl,
      Map<String, StructureField<HapiConverter<Schema>>> contained) {

    String recordName = DefinitionVisitorsUtil.recordNameFor(elementPath);
    String recordNamespace = DefinitionVisitorsUtil.namespaceFor(basePackage, elementTypeUrl);

    List<Field> fields =
        contained.values().stream()
            .map(
                containedEntry -> {
                  String doc = "Field for FHIR property " + containedEntry.propertyName();

                  return new Field(
                      containedEntry.fieldName(),
                      nullable(containedEntry.result().getDataType()),
                      doc,
                      JsonProperties.NULL_VALUE);
                })
            .collect(Collectors.toList());

    Schema containerType =
        Schema.createArray(
            Schema.createRecord(
                recordName, "Structure for FHIR type contained", recordNamespace, false, fields));

    return new HapiContainedToAvroConverter(contained, containerType);
  }

  @Override
  public HapiConverter<Schema> visitComposite(
      String elementName,
      String elementPath,
      String baseType,
      String elementTypeUrl,
      List<StructureField<HapiConverter<Schema>>> children) {

    Preconditions.checkArgument(!children.isEmpty());
    String recordName = DefinitionVisitorsUtil.recordNameFor(elementPath);
    String recordNamespace = DefinitionVisitorsUtil.namespaceFor(basePackage, elementTypeUrl);
    String fullName = recordNamespace + "." + recordName;

    HapiConverter<Schema> converter = visitedConverters.get(fullName);

    if (converter == null) {
      converter =
          createCompositeConverter(
              baseType,
              recordName,
              "Structure for FHIR type " + baseType,
              recordNamespace,
              children,
              fhirSupport,
              null);
      visitedConverters.put(fullName, converter);
    }

    return converter;
  }

  /** Field setter that does nothing for synthetic or unsupported field types. */
  private static class NoOpFieldSetter implements HapiFieldSetter, HapiObjectConverter {

    @Override
    public void setField(
        IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object sparkObject) {}

    @Override
    @Nullable // TODO superclass returns non-nullable
    public IBase toHapi(Object input) {
      return null;
    }
  }

  private static final HapiFieldSetter NOOP_FIELD_SETTER = new NoOpFieldSetter();

  /** Converter that returns the relative value of a URI type. */
  private static class RelativeValueConverter extends HapiConverter<Schema> {

    private final String prefix;

    RelativeValueConverter(String prefix) {
      this.prefix = prefix;
    }

    @Override
    @Nullable // TODO superclass returns non-nullable
    public Object fromHapi(Object input) {
      String uri = ((IPrimitiveType) input).getValueAsString();

      return uri != null && lowercase(uri).startsWith(lowercase(prefix))
          ? uri.substring(uri.lastIndexOf('/') + 1)
          : null;
    }

    @Override
    public Schema getDataType() {
      return Schema.create(Type.STRING);
    }

    @Override
    public String getElementType() {

      return "String";
    }

    @Override
    public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

      // Returns a field setter that does nothing, since this is for a synthetic type-specific
      // reference field, and the value will be set from the primary field.
      return NOOP_FIELD_SETTER;
    }

    @Override
    public HapiConverter<Schema> merge(HapiConverter<Schema> other) throws ProfileException {
      HapiConverterUtil.validateIfImplementationClassesAreSame(this, other);
      String leftPrefix = Strings.nullToEmpty(this.prefix);
      String rightPrefix = Strings.nullToEmpty(((RelativeValueConverter) other).prefix);
      if (!Objects.equals(leftPrefix, rightPrefix)) {
        throw new ProfileException(
            String.format(
                "RelativeValueConverter prefixes should be equal for merging, leftPrefix=%s,"
                    + " rightPrefix=%s",
                leftPrefix, rightPrefix));
      }
      // Return the current converter as the prefixes are same.
      return this;
    }
  }

  @Override
  public HapiConverter<Schema> visitReference(
      String elementName,
      List<String> referenceTypes,
      List<StructureField<HapiConverter<Schema>>> children) {

    // Generate a record name based on the element name.
    String recordName = elementName + "Reference";
    // Remove extension type [x] as this creates issue while validating schema.
    recordName = recordName.replaceAll("\\[x\\]", "");

    String fullName = basePackage + "." + recordName;

    HapiConverter<Schema> converter = visitedConverters.get(fullName);

    if (converter == null) {

      // Add direct references
      List<StructureField<HapiConverter<Schema>>> fieldsWithReferences =
          referenceTypes.stream()
              .map(
                  refUri -> {
                    String relativeType = refUri.substring(refUri.lastIndexOf('/') + 1);

                    // Convert to lower camel case if any of the element name is in upper camel
                    // case.
                    // E.g. OrganizationId to organizationId; To make it consistent with
                    // SQL-on-FHIR.
                    // https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#referenced
                    relativeType = lowercase(relativeType);

                    return new StructureField<HapiConverter<Schema>>(
                        "reference",
                        relativeType + "Id",
                        null,
                        false,
                        false,
                        new RelativeValueConverter(relativeType));
                  })
              .collect(Collectors.toList());

      fieldsWithReferences.addAll(children);

      List<Field> fields =
          fieldsWithReferences.stream()
              .map(
                  entry ->
                      new Field(
                          entry.fieldName(),
                          nullable(entry.result().getDataType()),
                          "Reference field",
                          JsonProperties.NULL_VALUE))
              .collect(Collectors.toList());

      Schema schema =
          Schema.createRecord(
              recordName, "Structure for FHIR type " + recordName, basePackage, false, fields);

      converter = new CompositeToAvroConverter(null, fieldsWithReferences, schema, fhirSupport);

      visitedConverters.put(fullName, converter);
    }

    return converter;
  }

  @Override
  public HapiConverter<Schema> visitParentExtension(
      String elementName,
      String extensionUrl,
      List<StructureField<HapiConverter<Schema>>> children) {

    String recordNamespace = DefinitionVisitorsUtil.namespaceFor(basePackage, extensionUrl);

    String localPart = extensionUrl.substring(extensionUrl.lastIndexOf('/') + 1);

    String recordName =
        Splitter.onPattern("[-|_]")
            .splitToStream(localPart)
            .map(part -> part.substring(0, 1).toUpperCase(Locale.ROOT) + part.substring(1))
            .collect(Collectors.joining());

    String fullName = recordNamespace + "." + recordName;

    HapiConverter<Schema> converter = visitedConverters.get(fullName);

    if (converter == null) {

      List<Field> fields =
          children.stream()
              .map(
                  entry ->
                      new Field(
                          entry.fieldName(),
                          nullable(entry.result().getDataType()),
                          "Doc here",
                          JsonProperties.NULL_VALUE))
              .collect(Collectors.toList());

      Schema schema =
          Schema.createRecord(recordName, "Reference type.", recordNamespace, false, fields);

      converter = new CompositeToAvroConverter(null, children, schema, fhirSupport, extensionUrl);

      visitedConverters.put(fullName, converter);
    }

    return converter;
  }

  @Override
  public HapiConverter<Schema> visitLeafExtension(
      String elementName, String extensionUrl, HapiConverter<Schema> element) {

    return new LeafExtensionConverter<Schema>(extensionUrl, element);
  }

  @Override
  public HapiConverter<Schema> visitMultiValued(
      String elementName, HapiConverter<Schema> arrayElement) {

    return new MultiValuedToAvroConverter(arrayElement);
  }

  @Override
  public HapiConverter<Schema> visitChoice(
      String elementName, Map<String, HapiConverter<Schema>> choiceTypes) {

    String fieldTypesName = createChoiceFieldTypesName(choiceTypes);
    String fullName = basePackage + "." + "Choice" + fieldTypesName;
    HapiConverter<Schema> converter = visitedConverters.get(fullName);
    if (converter == null) {
      converter = createChoiceConverter(choiceTypes, basePackage, fhirSupport);
      visitedConverters.put(fullName, converter);
    }
    return converter;
  }

  private static String lowercase(String string) {

    if (string.length() == 0) {

      return string;
    }

    return Character.toLowerCase(string.charAt(0)) + string.substring(1);
  }

  @Override
  public int getMaxDepth(String elementTypeUrl, String path) {
    return Math.max(1, this.recursiveDepth);
  }

  private static HapiCompositeConverter createCompositeConverter(
      String elementType,
      String recordName,
      String doc,
      String namespace,
      List<StructureField<HapiConverter<Schema>>> children,
      FhirConversionSupport fhirSupport,
      @Nullable String extensionUrl) {

    List<Field> fields =
        children.stream()
            .map(
                (StructureField<HapiConverter<Schema>> field) -> {
                  String desc =
                      recordName.endsWith("Reference")
                          ? "Reference field"
                          : field.extensionUrl() != null
                              ? "Extension field for " + field.extensionUrl()
                              : "Field for FHIR property " + field.propertyName();

                  return new Field(
                      field.fieldName(),
                      nullable(field.result().getDataType()),
                      desc,
                      JsonProperties.NULL_VALUE);
                })
            .collect(Collectors.toList());

    Schema schema = Schema.createRecord(recordName, doc, namespace, false, fields);

    return new CompositeToAvroConverter(elementType, children, schema, fhirSupport, extensionUrl);
  }

  private static HapiChoiceConverter createChoiceConverter(
      Map<String, HapiConverter<Schema>> choiceTypes,
      String namespace,
      FhirConversionSupport fhirSupport) {

    List<Field> fields = createChoiceFields(choiceTypes);
    String fieldTypesString = createChoiceFieldTypesName(choiceTypes);

    Schema schema =
        Schema.createRecord(
            "Choice" + fieldTypesString,
            "Structure for FHIR choice type ",
            namespace,
            false,
            fields);
    return new HapiChoiceToAvroConverter(choiceTypes, schema, fhirSupport);
  }

  private static List<Field> createChoiceFields(Map<String, HapiConverter<Schema>> choiceTypes) {
    return choiceTypes.entrySet().stream()
        .map(
            entry -> {
              // Ensure first character of the field is lower case.
              String fieldName = lowercase(entry.getKey());
              return new Field(
                  fieldName,
                  nullable(entry.getValue().getDataType()),
                  "Choice field",
                  JsonProperties.NULL_VALUE);
            })
        .collect(Collectors.toList());
  }

  private static String createChoiceFieldTypesName(Map<String, HapiConverter<Schema>> choiceTypes) {
    return choiceTypes.entrySet().stream()
        .map(
            choiceEntry -> {
              // References need their full record name, which includes the permissible referent
              // types
              if (choiceEntry.getKey().equals("Reference")) {
                return choiceEntry.getValue().getDataType().getName();
              } else {
                return choiceEntry.getKey();
              }
            })
        .sorted()
        .map(StringUtils::capitalize)
        .collect(Collectors.joining());
  }
}
