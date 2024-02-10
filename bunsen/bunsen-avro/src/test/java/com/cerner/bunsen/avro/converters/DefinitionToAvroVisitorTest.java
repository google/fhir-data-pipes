package com.cerner.bunsen.avro.converters;

import com.cerner.bunsen.definitions.StructureDefinitions;
import org.junit.Assert;
import org.junit.Test;

public class DefinitionToAvroVisitorTest {

  @Test
  public void checkPrimitivesMap() {
    Assert.assertEquals(
        DefinitionToAvroVisitor.TYPE_TO_CONVERTER.keySet(), StructureDefinitions.PRIMITIVE_TYPES);
  }
}
