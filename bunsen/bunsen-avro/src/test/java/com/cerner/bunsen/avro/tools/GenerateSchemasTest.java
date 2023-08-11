package com.cerner.bunsen.avro.tools;

import com.cerner.bunsen.stu3.TestData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import org.junit.Assert;
import org.junit.Test;

// TODO duplicate these tests for R4 as well.
public class GenerateSchemasTest {

  private static final String BASE_TEST_URL = "http://test.org/test_resource";

  @Test
  public void testWriteSchema() throws IOException {

    Path generatedCodePath = Files.createTempDirectory("schema_directory");

    generatedCodePath.toFile().deleteOnExit();

    Path outputFile = generatedCodePath.resolve("out.asvc");

    int result =
        GenerateSchemas.main(
            new String[] {
              outputFile.toString(),
              TestData.US_CORE_PATIENT,
              TestData.US_CORE_CONDITION,
              TestData.US_CORE_MEDICATION,
              TestData.US_CORE_MEDICATION_REQUEST
            });

    Assert.assertEquals(0, result);

    Assert.assertTrue(outputFile.toFile().exists());
  }

  @Test
  public void testGenerateContainedUrlsWithEmptyStringReturnsEmptyList() {
    Assert.assertEquals(Collections.emptyList(), GenerateSchemas.generateContainedUrls(""));
  }

  @Test
  public void testGenerateContainedUrlsWithStringWithoutSemicolonReturnsEmptyList() {
    String testResource = BASE_TEST_URL;
    Assert.assertEquals(
        Collections.emptyList(), GenerateSchemas.generateContainedUrls(testResource));
  }

  @Test
  public void testGenerateContainedUrlsWithStringWithOneContainedResource() {
    String testResourceWithContained =
        BASE_TEST_URL + GenerateSchemas.DELIMITER + "http://test.org/test_contained";
    List<String> testContainedList =
        GenerateSchemas.generateContainedUrls(testResourceWithContained);

    Assert.assertEquals(1, testContainedList.size());
    Assert.assertEquals("http://test.org/test_contained", testContainedList.get(0));
  }

  @Test
  public void testGenerateContainedUrlWithStringWithMultipleContainedResources() {
    StringJoiner joiner = new StringJoiner(GenerateSchemas.DELIMITER);
    joiner.add(BASE_TEST_URL);
    String[] originalList = new String[10];

    for (int x = 0; x < 10; x++) {
      String curr = "http://test.org/contained_" + x;
      originalList[x] = curr;
      joiner.add(curr);
    }

    List<String> testContainedList = GenerateSchemas.generateContainedUrls(joiner.toString());

    Assert.assertEquals(Arrays.asList(originalList), testContainedList);
  }
}
