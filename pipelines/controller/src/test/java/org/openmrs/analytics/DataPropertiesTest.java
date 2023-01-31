/*
 * Copyright 2020-2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.openmrs.analytics;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class DataPropertiesTest {
  @Autowired private DataProperties dataProperties;

  @Test
  void outputParquetFilePath_has_allUnderscores() {
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    Assert.assertEquals(pipelineConfig.getThriftServerParquetPath().indexOf(":"), -1);
    Assert.assertEquals(pipelineConfig.getThriftServerParquetPath().indexOf("-"), -1);
    Assert.assertEquals(pipelineConfig.getThriftServerParquetPath().indexOf("."), -1);
    Assert.assertNotEquals(pipelineConfig.getThriftServerParquetPath().indexOf("_"), -1);
  }

  @Test
  void outputParquetFilePath_has_rightTimestampSuffix() {
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    String timestampSuffix = pipelineConfig.getTimestampSuffix();
    Assert.assertTrue(
        pipelineConfig.getFhirEtlOptions().getOutputParquetPath().contains(timestampSuffix));
  }

  @Test
  void thriftServerParquetFilePath_has_rightTimestampSuffix() {
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    String timestampSuffix = pipelineConfig.getTimestampSuffix();
    Assert.assertTrue(pipelineConfig.getThriftServerParquetPath().contains(timestampSuffix));
  }
}
