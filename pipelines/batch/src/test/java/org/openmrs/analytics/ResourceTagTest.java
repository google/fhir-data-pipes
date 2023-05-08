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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.commons.lang3.SerializationUtils;
import org.hl7.fhir.r4.model.Coding;
import org.junit.Test;

public class ResourceTagTest {

  @Test
  public void testResourceTagSerialization() {
    Coding coding = new Coding();
    coding.setId("123");
    coding.setDisplay("display123");
    coding.setSystem("system123");
    ResourceTag resourceTag = new ResourceTag(coding, "resourceId", 1);

    byte[] bytes = SerializationUtils.serialize(resourceTag);
    ResourceTag deserializedResourceTag = SerializationUtils.deserialize(bytes);

    assertThat(
        resourceTag.getCoding().getId(), equalTo(deserializedResourceTag.getCoding().getId()));
    assertThat(
        resourceTag.getCoding().getDisplay(),
        equalTo(deserializedResourceTag.getCoding().getDisplay()));
    assertThat(
        resourceTag.getCoding().getSystem(),
        equalTo(deserializedResourceTag.getCoding().getSystem()));
    assertThat(resourceTag.getResourceId(), equalTo(deserializedResourceTag.getResourceId()));
    assertThat(resourceTag.getTagType(), equalTo(deserializedResourceTag.getTagType()));
  }
}
