package com.cerner.bunsen.definitions.r4;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.definitions.FhirConversionSupport;
import com.cerner.bunsen.definitions.IElementDefinition;
import com.cerner.bunsen.definitions.IStructureDefinition;
import com.cerner.bunsen.definitions.StructureDefinitions;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.ElementDefinition;
import org.hl7.fhir.r4.model.StructureDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: A significant part of this is similar to Stu3StructureDefinitions which we should refactor.
// This is non-trivial because FHIR ElementDefinition objects do not share the same interface for
// different FHIR versions.

/** {@link StructureDefinitions} implementation for FHIR R4. */
public class R4StructureDefinitions extends StructureDefinitions {

  private static final Logger log = LoggerFactory.getLogger(R4StructureDefinitions.class);

  private static final FhirConversionSupport CONVERSION_SUPPORT = new R4FhirConversionSupport();

  public R4StructureDefinitions(FhirContext context) {

    super(context);
  }

  @Override
  public FhirConversionSupport conversionSupport() {

    return CONVERSION_SUPPORT;
  }

  @Override
  protected IStructureDefinition getStructureDefinition(String resourceUrl) {
    return new StructureDefinitionWrapper(
        (StructureDefinition) context.getValidationSupport().fetchStructureDefinition(resourceUrl));
  }

  // FHIR version specific interface implementations

  private static class StructureDefinitionWrapper implements IStructureDefinition {
    private final StructureDefinition structureDefinition;

    StructureDefinitionWrapper(StructureDefinition structureDefinition) {
      this.structureDefinition = structureDefinition;
    }

    @Override
    public String getUrl() {
      return structureDefinition.getUrl();
    }

    @Override
    public String getType() {
      return structureDefinition.getType();
    }

    @Override
    public IElementDefinition getRootDefinition() {
      return new ElementDefinitionWrapper(structureDefinition.getSnapshot().getElementFirstRep());
    }

    @Override
    public List<IElementDefinition> getSnapshotDefinitions() {
      return structureDefinition.getSnapshot().getElement().stream()
          .map(d -> new ElementDefinitionWrapper(d))
          .collect(Collectors.toList());
    }
  }

  private static class ElementDefinitionWrapper implements IElementDefinition {
    private final ElementDefinition elementDefinition;

    ElementDefinitionWrapper(ElementDefinition elementDefinition) {
      this.elementDefinition = elementDefinition;
    }

    @Override
    public String getId() {
      return elementDefinition.getId();
    }

    @Override
    public String getPath() {
      return elementDefinition.getPath();
    }

    @Override
    public String getContentReference() {
      return elementDefinition.getContentReference();
    }

    @Override
    public String getMax() {
      return elementDefinition.getMax();
    }

    @Override
    public String getFirstTypeCode() {
      return elementDefinition.getTypeFirstRep().getCode();
    }

    @Override
    public boolean hasSingleType() {
      return (elementDefinition.getType().size() == 1);
    }

    @Override
    public List<String> getAllTypeCodes() {
      return elementDefinition.getType().stream()
          .map(t -> t.getCode())
          .collect(Collectors.toList());
    }

    @Override
    public String getSliceName() {
      return elementDefinition.getSliceName();
    }

    @Override
    public boolean getIsModifier() {
      return elementDefinition.getIsModifier();
    }

    @Override
    public String getFixedPrimitiveValue() {
      if (elementDefinition.getFixed() == null) {
        return null;
      }
      return elementDefinition.getFixed().primitiveValue();
    }

    @Override
    public List<String> getReferenceTargetProfiles() {
      return elementDefinition.getType().stream()
          .filter(type -> "Reference".equals(type.getCode()))
          .filter(type -> type.getTargetProfile() != null)
          .map(type -> type.getTargetProfile())
          // Note there is a difference between how `Reference` types are represented in R4 vs STU3,
          // in R4 all "target profiles" (i.e., reference's target type) are under the same `type`
          // while in STU3 we have multiple `type` each with a single target-profile.
          .flatMap(Collection::stream)
          .map(profile -> profile.getValue())
          .collect(Collectors.toList());
    }

    @Override
    public String getFirstTypeProfile() {
      List<CanonicalType> profiles = elementDefinition.getTypeFirstRep().getProfile();
      if (profiles == null || profiles.isEmpty()) {
        return null;
      }
      return profiles.get(0).getValue();
    }

    @Override
    public String toString() {
      return elementDefinition.toString();
    }
  }
}
