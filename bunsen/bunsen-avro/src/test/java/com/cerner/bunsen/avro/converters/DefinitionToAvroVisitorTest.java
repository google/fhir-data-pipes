package com.cerner.bunsen.avro.converters;

import com.cerner.bunsen.definitions.StructureDefinitions;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class DefinitionToAvroVisitorTest {

  @Test
  public void checkPrimitivesMap() {
    Set<String> primitives = new HashSet<>(DefinitionToAvroVisitor.TYPE_TO_CONVERTER.keySet());
    primitives.add("id");
    Assert.assertEquals(primitives, StructureDefinitions.PRIMITIVE_TYPES);
  }
}
