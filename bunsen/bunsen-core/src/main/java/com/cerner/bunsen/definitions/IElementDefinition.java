package com.cerner.bunsen.definitions;

import java.util.List;
import javax.annotation.Nullable;

/**
 * This is the minimal interface that is needed from ElementDefinition for doing the type structure
 * traversal. The expected implementation pattern is that a version specific ElementDefinition will
 * be encapsulated by a corresponding IElementDefinition.
 */
public interface IElementDefinition {

  String getId();

  String getPath();

  String getContentReference();

  String getMax();

  // Note the type codes are FHIR version dependent and are also extensible, see:
  // https://hl7.org/fhir/valueset-elementdefinition-types.html
  String getFirstTypeCode();

  /**
   * See return below.
   *
   * @return true iff this element has exactly one type (e.g., type does not end in `[x]`).
   */
  boolean hasSingleType();

  List<String> getAllTypeCodes();

  String getSliceName();

  boolean getIsModifier();

  // TODO remove this once we properly handle extension URL extraction.
  @Nullable
  String getFixedPrimitiveValue();

  /**
   * See return below.
   *
   * @return list of `targetProfile`s for each `type` that is a reference.
   */
  List<String> getReferenceTargetProfiles();

  /**
   * This is a helper to address differences of `ElementDefinition.TypeRefComponent.getProfile()` in
   * R4 vs. STU3 versions which returns a {@code List<CanonicalType>} in the former but a String in
   * the latter. This is due to the difference between how `Reference` types are represented in R4
   * vs STU3; in R4 all "target profiles" (i.e., reference's target type) are under the same `type`
   * while in STU3 we have multiple `type` each with a single target-profile.
   *
   * @return first profile or null if this element has no profiles
   */
  @Nullable
  String getFirstTypeProfile();
}
