package com.cerner.bunsen.definitions;

import com.cerner.bunsen.exception.ProfileException;

/** Util class which provides helper methods for the HapiConverter class. */
public class HapiConverterUtil {

  /** Validates if the given left and right HapiConverter implementation classes are same. */
  public static void validateIfImplementationClassesAreSame(HapiConverter left, HapiConverter right)
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
  }
}
