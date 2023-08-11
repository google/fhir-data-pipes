package com.cerner.bunsen.definitions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.hl7.fhir.dstu3.model.IdType;
import org.junit.Before;
import org.junit.Test;

public class IdConverterTest {

  private IdConverter<ArbitraryDataType> idConverter;

  @Before
  public void setUp() {
    idConverter = new IdConverter<ArbitraryDataType>(new ArbitraryDataType());
  }

  @Test
  public void relativePathTest() {
    assertThat(idConverter.fromHapi(new IdType("Patinet/123")), equalTo("123"));
  }

  @Test
  public void absoluteUrlTest() {
    assertThat(idConverter.fromHapi(new IdType("http://fhir.server/Patinet/123")), equalTo("123"));
  }

  @Test
  public void idPartTest() {
    assertThat(idConverter.fromHapi(new IdType("123")), equalTo("123"));
  }

  // In real scenarios this would be the specific type to/from which the HAPI conversion happens,
  // e.g., an Avro Schema when the conversion is to/from Avro records.
  private static class ArbitraryDataType {}
}
