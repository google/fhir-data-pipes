package com.cerner.bunsen.definitions;

import com.cerner.bunsen.exception.ProfileException;
import com.google.common.base.Strings;
import java.util.Objects;

/** Util class which provides helper methods for the HapiConverter class. */
public class HapiConverterUtil {

  /** Validates if the given left and right HapiConverters are compatible to be merged. */
  public static void validateIfConvertersCanBeMerged(HapiConverter left, HapiConverter right)
      throws ProfileException {
    if (left == null
        || right == null
        || !left.getClass().getName().equals(right.getClass().getName())) {
      throw new ProfileException(
          String.format(
              "Cannot merge %s with %s",
              left == null ? null : left.getClass().getName(),
              right == null ? null : right.getClass().getName()));
    }

    if (left instanceof PrimitiveConverter
        && !Objects.equals(left.getElementType(), right.getElementType())) {
      throw new ProfileException(
          String.format(
              "Cannot merge %s FHIR Type %s with %s FHIR Type %s",
              left.getElementType(),
              left.getClass().getName(),
              right.getElementType(),
              right.getClass().getName()));
    }

    if (left instanceof LeafExtensionConverter
        && (Strings.isNullOrEmpty(left.extensionUrl())
            || Strings.isNullOrEmpty(right.extensionUrl())
            || !Objects.equals(left.extensionUrl(), right.extensionUrl()))) {
      throw new ProfileException(
          String.format(
              "Extension urls must not be empty and should be the same for merging,"
                  + " currentExtensionUrl=%s, otherExtensionUrl=%s",
              left.extensionUrl(), right.extensionUrl()));
    }
  }
}
