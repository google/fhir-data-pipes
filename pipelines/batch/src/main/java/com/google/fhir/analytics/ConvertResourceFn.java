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

import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.DataFormatException;
import com.google.common.base.Strings;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.codesystems.ActionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertResourceFn extends FetchSearchPageFn<HapiRowDescriptor> {

  private static final Logger log = LoggerFactory.getLogger(ConvertResourceFn.class);

  private final SimpleDateFormat simpleDateFormat;

  private final HashMap<String, Counter> numFetchedResourcesMap;

  private final HashMap<String, Counter> totalParseTimeMillisMap;

  private final HashMap<String, Counter> totalGenerateTimeMillisMap;

  private final HashMap<String, Counter> totalPushTimeMillisMap;

  private final Boolean processDeletedRecords;

  Counter counter =
      Metrics.counter(
          MetricsConstants.METRICS_NAMESPACE, MetricsConstants.DATA_FORMAT_EXCEPTION_ERROR);

  ConvertResourceFn(FhirEtlOptions options, String stageIdentifier) {
    super(options, stageIdentifier);
    this.numFetchedResourcesMap = new HashMap<String, Counter>();
    this.totalParseTimeMillisMap = new HashMap<String, Counter>();
    this.totalGenerateTimeMillisMap = new HashMap<String, Counter>();
    this.totalPushTimeMillisMap = new HashMap<String, Counter>();
    // Only in the incremental mode we process deleted resources.
    this.processDeletedRecords = !Strings.isNullOrEmpty(options.getSince());
    List<String> resourceTypes = Arrays.asList(options.getResourceList().split(","));
    for (String resourceType : resourceTypes) {
      this.numFetchedResourcesMap.put(
          resourceType,
          Metrics.counter(
              MetricsConstants.METRICS_NAMESPACE,
              MetricsConstants.NUM_FETCHED_RESOURCES + resourceType));
      this.totalParseTimeMillisMap.put(
          resourceType,
          Metrics.counter(
              MetricsConstants.METRICS_NAMESPACE,
              MetricsConstants.TOTAL_PARSE_TIME_MILLIS + resourceType));
      this.totalGenerateTimeMillisMap.put(
          resourceType,
          Metrics.counter(
              MetricsConstants.METRICS_NAMESPACE,
              MetricsConstants.TOTAL_GENERATE_TIME_MILLIS + resourceType));
      this.totalPushTimeMillisMap.put(
          resourceType,
          Metrics.counter(
              MetricsConstants.METRICS_NAMESPACE,
              MetricsConstants.TOTAL_PUSH_TIME_MILLIS + resourceType));
    }
    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
  }

  public void writeResource(HapiRowDescriptor element)
      throws IOException, ParseException, SQLException {
    String resourceId = element.resourceId();
    String forcedId = element.forcedId();
    String resourceType = element.resourceType();
    Meta meta =
        new Meta()
            .setVersionId(element.resourceVersion())
            .setLastUpdated(simpleDateFormat.parse(element.lastUpdated()));
    setMetaTags(element, meta);
    String jsonResource = element.jsonResource();
    long startTime = System.currentTimeMillis();
    Resource resource = null;
    if (jsonResource == null || jsonResource.isBlank()) {
      // The jsonResource field will be empty in case of deleted records and are ignored during
      // the initial batch run
      if (!processDeletedRecords) {
        return;
      }

      // For incremental run, create a new resource and attach the meta field with a deleted tag,
      // this deleted tag is later used in the merge process to identify if the record has been
      // deleted
      resource = createNewFhirResource(element.fhirVersion(), resourceType);
      ActionType removeAction = ActionType.REMOVE;
      meta.setLastUpdated(new Date());
      meta.addTag(
          new Coding(removeAction.getSystem(), removeAction.toCode(), removeAction.getDisplay()));
    } else {
      try {
        resource = (Resource) parser.parseResource(jsonResource);
      } catch (DataFormatException e) {
        log.error("DataFormatException Error occurred", e);
        counter.inc();
        return;
      }
    }
    totalParseTimeMillisMap.get(resourceType).inc(System.currentTimeMillis() - startTime);
    if (forcedId == null || forcedId.equals("")) {
      resource.setId(resourceId);
    } else {
      resource.setId(forcedId);
    }
    resource.setMeta(meta);

    numFetchedResourcesMap.get(resourceType).inc(1);

    if (!parquetFile.isEmpty()) {
      startTime = System.currentTimeMillis();
      parquetUtil.write(resource);
      totalGenerateTimeMillisMap.get(resourceType).inc(System.currentTimeMillis() - startTime);
    }
    if (!this.sinkPath.isEmpty()) {
      startTime = System.currentTimeMillis();
      // TODO : Remove the deleted resources from the sink fhir store
      // https://github.com/google/fhir-data-pipes/issues/588
      fhirStoreUtil.uploadResource(resource);
      totalPushTimeMillisMap.get(resourceType).inc(System.currentTimeMillis() - startTime);
    }
    if (this.sinkDbConfig != null) {
      // TODO : Remove the deleted resources from the sink database
      // https://github.com/google/fhir-data-pipes/issues/588
      jdbcWriter.writeResource(resource);
    }
  }

  private void setMetaTags(HapiRowDescriptor element, Meta meta) {
    if (element.getTags() != null) {
      List<Coding> tags = new ArrayList<>();
      List<Coding> securityList = new ArrayList<>();
      List<CanonicalType> profiles = new ArrayList<>();
      for (ResourceTag resourceTag : element.getTags()) {
        // The HAPI FHIR tagType of value 0 means it's of type TAG, 1 for PROFILE and 2 for SYSTEM.
        // https://hapifhir.io/hapi-fhir/apidocs/hapi-fhir-jpaserver-model/ca/uhn/fhir/jpa/model/entity/TagTypeEnum.html
        if (resourceTag.getTagType() == 1) {
          CanonicalType canonicalType = new CanonicalType();
          canonicalType.setValue(resourceTag.getCoding().getCode());
          profiles.add(canonicalType);
        } else if (resourceTag.getTagType() == 0) {
          tags.add(resourceTag.getCoding());
        } else if (resourceTag.getTagType() == 2) {
          securityList.add(resourceTag.getCoding());
        }
      }

      if (!profiles.isEmpty()) {
        meta.setProfile(profiles);
      }
      if (!tags.isEmpty()) {
        meta.setTag(tags);
      }
      if (!securityList.isEmpty()) {
        meta.setSecurity(securityList);
      }
    }
  }

  @ProcessElement
  public void processElement(ProcessContext processContext)
      throws IOException, ParseException, SQLException {
    HapiRowDescriptor element = processContext.element();
    writeResource(element);
  }

  private Resource createNewFhirResource(String fhirVersion, String resourceType) {
    try {
      return (Resource)
          Class.forName(getFhirBasePackageName(fhirVersion) + "." + resourceType)
              .getConstructor()
              .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException e) {
      String errorMessage =
          String.format("Failed to instantiate new FHIR resource of type %s", resourceType);
      log.error(errorMessage, e);
      throw new FHIRException(errorMessage, e);
    }
  }

  private String getFhirBasePackageName(String fhirVersion) {
    FhirVersionEnum fhirVersionEnum = FhirVersionEnum.forVersionString(fhirVersion);
    switch (fhirVersionEnum) {
      case DSTU2:
        return org.hl7.fhir.dstu2.model.Base.class.getPackageName();
      case DSTU2_1:
        return org.hl7.fhir.dstu2016may.model.Base.class.getPackageName();
      case DSTU3:
        return org.hl7.fhir.dstu3.model.Base.class.getPackageName();
      case R4:
        return org.hl7.fhir.r4.model.Base.class.getPackageName();
      case R4B:
        return org.hl7.fhir.r4b.model.Base.class.getPackageName();
      case R5:
        return org.hl7.fhir.r5.model.Base.class.getPackageName();
      default:
        String errorMessage = String.format("Invalid fhir version %s", fhirVersion);
        log.error(errorMessage);
        throw new FHIRException(errorMessage);
    }
  }
}
