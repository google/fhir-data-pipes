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
package com.google.fhir.analytics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.commons.lang3.SerializationUtils;
import org.hl7.fhir.r4.model.Coding;
import org.junit.Assert;
import org.junit.Test;

public class ResourceTagTest {

  @Test
  public void testResourceTagSerialization() {
    Coding coding = new Coding();
    coding.setId("123");
    coding.setDisplay("display123");
    coding.setSystem("system123");
    ResourceTag resourceTag =
        ResourceTag.builder().coding(coding).resourceId("resourceId").tagType(1).build();

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

  @Test
  public void testResourceTagSerializationWithoutResourceId() {
    Coding coding = new Coding();
    coding.setId("123");
    coding.setDisplay("display123");
    coding.setSystem("system123");
    ResourceTag resourceTag = ResourceTag.builder().coding(coding).tagType(1).build();

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

  @Test
  public void testResourceTagSerializationWithoutTagType() {
    Coding coding = new Coding();
    coding.setId("123");
    coding.setDisplay("display123");
    coding.setSystem("system123");
    ResourceTag resourceTag = ResourceTag.builder().coding(coding).resourceId("resourceId").build();

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

  @Test
  public void testResourceTagSerializationWithoutCoding() {

    ResourceTag resourceTag = ResourceTag.builder().resourceId("resourceId").tagType(1).build();

    byte[] bytes = SerializationUtils.serialize(resourceTag);
    ResourceTag deserializedResourceTag = SerializationUtils.deserialize(bytes);

    assertThat(resourceTag.getResourceId(), equalTo(deserializedResourceTag.getResourceId()));
    assertThat(resourceTag.getTagType(), equalTo(deserializedResourceTag.getTagType()));
  }

  @Test
  public void testResourceTagEqualsAndHashCode() {
    Coding coding = new Coding();
    coding.setId("123");
    coding.setDisplay("display123");
    coding.setSystem("system123");
    ResourceTag resourceTag1 =
        ResourceTag.builder().coding(coding).resourceId("resourceId").tagType(1).build();
    ResourceTag resourceTag2 =
        ResourceTag.builder().coding(coding).resourceId("resourceId").tagType(1).build();

    Assert.assertTrue(resourceTag1.equals(resourceTag2) && resourceTag2.equals(resourceTag1));
    Assert.assertTrue(resourceTag1.hashCode() == resourceTag2.hashCode());
  }
}
