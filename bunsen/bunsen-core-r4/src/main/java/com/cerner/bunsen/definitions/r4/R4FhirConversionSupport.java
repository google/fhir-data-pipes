package com.cerner.bunsen.definitions.r4;

import com.cerner.bunsen.definitions.FhirConversionSupport;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Property;

/** Conversion support for translating STU3 FHIR resources. */
public class R4FhirConversionSupport extends FhirConversionSupport {

  @Override
  public String fhirType(IBase base) {

    return ((Base) base).fhirType();
  }

  @Override
  public Map<String, List> compositeValues(IBase composite) {

    List<Property> children = ((Base) composite).children();

    if (children == null) {

      return null;
    } else {

      // Some FHIR resources produce duplicate properties in the children,
      // so just use the first when converting to a map.
      return children.stream()
          .filter(property -> property.hasValues())
          .collect(
              Collectors.toMap(
                  Property::getName, property -> property.getValues(), (first, second) -> first));
    }
  }

  @Override
  public List<IBaseResource> extractEntryFromBundle(IBaseBundle bundle, String resourceName) {

    Bundle stu3Bundle = (Bundle) bundle;

    List<IBaseResource> items =
        stu3Bundle.getEntry().stream()
            .map(BundleEntryComponent::getResource)
            .filter(
                resource ->
                    resource != null
                        && resourceName.equalsIgnoreCase(resource.getResourceType().name()))
            .collect(Collectors.toList());

    return items;
  }
}
