/*
 * Copyright 2020-2024 Google LLC
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.SerializationUtils;
import org.hl7.fhir.r4.model.Coding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcFetchHapi {

  private static final Logger log = LoggerFactory.getLogger(JdbcFetchHapi.class);

  private DataSource jdbcSource;

  JdbcFetchHapi(DataSource jdbcSource) {
    this.jdbcSource = jdbcSource;
  }

  /**
   * RowMapper class implementation for JdbcIo direct fetch with HAPI as the source FHIR server.
   * Each element in the ResultSet returned by the query maps to a List of String objects
   * corresponding to the column values in the query result.
   */
  public static class ResultSetToRowDescriptor implements JdbcIO.RowMapper<HapiRowDescriptor> {

    private final HashMap<String, Counter> numMappedResourcesMap;

    public ResultSetToRowDescriptor(String resourceList) {
      this.numMappedResourcesMap = new HashMap<>();
      List<String> resourceTypes = Arrays.asList(resourceList.split(","));
      for (String resourceType : resourceTypes) {
        this.numMappedResourcesMap.put(
            resourceType,
            Metrics.counter(
                MetricsConstants.METRICS_NAMESPACE,
                MetricsConstants.NUM_MAPPED_RESOURCES + resourceType));
      }
    }

    @Override
    public HapiRowDescriptor mapRow(ResultSet resultSet) throws Exception {
      String jsonResource = "";

      // TODO check for null values before accessing columns; this caused NPEs with `latest` HAPI.
      switch (resultSet.getString("res_encoding")) {
        case "JSON":
          jsonResource = new String(resultSet.getBytes("res_text_vc"), Charsets.UTF_8);
          break;
        case "JSONC":
          Blob blob = resultSet.getBlob("res_text");
          jsonResource = GZipUtil.decompress(blob.getBytes(1, (int) blob.length()));
          blob.free();
          break;
        case "DEL":
          break;
      }

      String resourceId = resultSet.getString("res_id");
      String fhirId = resultSet.getString("fhir_id");
      String forcedId = resultSet.getString("forced_id");
      String resourceType = resultSet.getString("res_type");
      String lastUpdated = resultSet.getString("res_updated");
      String fhirVersion = resultSet.getString("res_version");
      String resourceVersion = resultSet.getString("res_ver");
      numMappedResourcesMap.get(resourceType).inc();
      return HapiRowDescriptor.create(
          resourceId,
          fhirId,
          forcedId,
          resourceType,
          lastUpdated,
          fhirVersion,
          resourceVersion,
          jsonResource);
    }
  }

  public static class CodingToRowDescriptor implements JdbcIO.RowMapper<ResourceTag> {

    @Override
    public ResourceTag mapRow(ResultSet resultSet) throws Exception {

      Coding coding =
          new Coding(
              resultSet.getString("tag_system"),
              resultSet.getString("tag_code"),
              resultSet.getString("tag_display"));

      return ResourceTag.builder()
          .coding(coding)
          .resourceId(resultSet.getString("res_id"))
          .tagType(resultSet.getInt("tag_type"))
          .build();
    }
  }

  public static class MdmLinkToRowDescriptor implements JdbcIO.RowMapper<MdmLink> {

    @Override
    public MdmLink mapRow(ResultSet resultSet) throws Exception {
      return MdmLink.builder()
          .sourceFhirId(resultSet.getString("source_fhir_id"))
          .goldenFhirId(resultSet.getString("golden_fhir_id"))
          .resourceId(resultSet.getString("src_resource_id"))
          .build();
    }
  }

  public static class SourceIdentifierToRowDescriptor
      implements JdbcIO.RowMapper<SourceIdentifier> {

    @Override
    public SourceIdentifier mapRow(ResultSet resultSet) throws Exception {
      return SourceIdentifier.builder()
          .resourceId(resultSet.getString("golden_resource_pid"))
          .system(resultSet.getString("sp_system"))
          .value(resultSet.getString("sp_value"))
          .build();
    }
  }

  /**
   * Utilizes Beam JdbcIO to query for resources directly from FHIR (HAPI) server's database and
   * returns a PCollection of Lists of String objects - each corresponding to a resource's payload
   */
  public static class FetchRowsJdbcIo
      extends PTransform<PCollection<QueryParameterDescriptor>, PCollection<HapiRowDescriptor>> {

    private final String resourceList;
    private final JdbcIO.DataSourceConfiguration dataSourceConfig;

    private final String query;

    private final String tagQuery;

    private final String mdmQuery;

    private final boolean isMdmResource;
    private final boolean mapToGoldenResources;
    private String identifiersQuery;

    public FetchRowsJdbcIo(
        String resourceList,
        JdbcIO.DataSourceConfiguration dataSourceConfig,
        String since,
        boolean isMdmResource,
        boolean mapToGoldenResource) {
      this.resourceList = resourceList;
      this.dataSourceConfig = dataSourceConfig;
      this.isMdmResource = isMdmResource;
      this.mapToGoldenResources = mapToGoldenResource;
      // Note the constraint on `res.res_ver` ensures we only pick the latest version.
      StringBuilder builder =
          new StringBuilder(
              "SELECT res.fhir_id, res.res_id, hfi.forced_id, res.res_type, res.res_updated,"
                  + " res.res_ver, res.res_version, ver.res_encoding, ver.res_text, ver.res_text_vc"
                  + " FROM hfj_resource res JOIN hfj_res_ver ver ON res.res_id = ver.res_id AND"
                  + " res.res_ver = ver.res_ver LEFT JOIN hfj_forced_id hfi ON res.res_id ="
                  + " hfi.resource_pid");
      if (isMdmResource) {
        builder.append(
            " LEFT JOIN hfj_res_tag tag ON tag.res_id = res.res_id JOIN hfj_tag_def tag_def"
                + " ON tag_def.tag_id = tag.tag_id"
                + " WHERE tag_def.tag_code = 'GOLDEN_RECORD' AND");
      } else {
        builder.append(" WHERE");
      }
      builder.append(" res.res_type = ? AND res.res_id % ? = ? AND ver.res_encoding != 'DEL'");
      // TODO do date sanity-checking on `since` (note this is partly done by HAPI client call).
      if (since != null && !since.isEmpty()) {
        builder.append(" AND res.res_updated > '").append(since).append("'");
      }
      query = builder.toString();
      log.info("JDBC query template for HAPI is " + query);

      builder =
          new StringBuilder(
              "SELECT td.tag_code, td.tag_display, td.tag_system, td.tag_type, tag.res_id FROM"
                  + " hfj_res_tag tag JOIN hfj_tag_def td ON tag.tag_id = td.tag_id JOIN"
                  + " hfj_resource res ON tag.res_id = res.res_id WHERE res.res_type = ? AND"
                  + " res.res_id % ? = ? ");
      if (since != null && !since.isEmpty()) {
        builder.append("  and res.res_updated > '").append(since).append("'");
      }
      tagQuery = builder.toString();
      log.info("JDBC query for tags: " + tagQuery);

      builder =
          new StringBuilder(
              "SELECT res_link.src_resource_id, golden.fhir_id AS golden_fhir_id, source.fhir_id AS"
                  + " source_fhir_id FROM hfj_res_link res_link JOIN mpi_link mdm_link ON"
                  + " res_link.target_resource_id = mdm_link.target_pid JOIN hfj_resource golden ON"
                  + " golden.res_id = mdm_link.golden_resource_pid JOIN hfj_resource source ON"
                  + " source.res_id = mdm_link.target_pid WHERE res_link.source_resource_type = ?"
                  + " AND res_link.src_resource_id % ? = ? AND"
                  + " mdm_link.match_result = 2 OR mdm_link.match_result = 1");
      mdmQuery = builder.toString();
      log.info("JDBC query for mdm: " + mdmQuery);

      if (isMdmResource) {
        builder =
            new StringBuilder(
                "SELECT identifier.sp_system, identifier.sp_value, mdm_link.golden_resource_pid"
                    + " FROM hfj_spidx_token identifier JOIN hfj_resource res ON identifier.res_id"
                    + " = res.res_id JOIN mpi_link mdm_link ON mdm_link.target_pid ="
                    + " identifier.res_id WHERE sp_name = 'identifier' AND identifier.sp_system !="
                    + " 'http://hapifhir.io/fhir/NamingSystem/mdm-golden-resource-enterprise-id'"
                    + " AND res.res_type = ? AND mdm_link.golden_resource_pid % ? = ?");

        identifiersQuery = builder.toString();
        log.info("JDBC query for source identifiers: " + identifiersQuery);
      }
    }

    @Override
    public PCollection<HapiRowDescriptor> expand(
        PCollection<QueryParameterDescriptor> queryParameters) {
      PCollection<HapiRowDescriptor> hapiRowDescriptorPCollection =
          createPCollection(
              queryParameters, "JdbcIO readAll", query, new ResultSetToRowDescriptor(resourceList));
      PCollection<ResourceTag> resourceTagPCollection =
          createPCollection(
              queryParameters, "JdbcIO fetch tags", tagQuery, new CodingToRowDescriptor());

      PCollection<MdmLink> mdmLinkPCollection = null;
      if (mapToGoldenResources) {
        mdmLinkPCollection =
            createPCollection(
                queryParameters, "JdbcIO fetch mdm links", mdmQuery, new MdmLinkToRowDescriptor());
      }

      PCollection<SourceIdentifier> sourceIdentifierPCollection = null;
      if (isMdmResource) {
        sourceIdentifierPCollection =
            createPCollection(
                queryParameters,
                "JdbcIO fetch source identifiers",
                identifiersQuery,
                new SourceIdentifierToRowDescriptor());
      }

      return joinResourceTagCollections(
          hapiRowDescriptorPCollection,
          resourceTagPCollection,
          mdmLinkPCollection,
          sourceIdentifierPCollection);
    }

    private <T> PCollection<T> createPCollection(
        PCollection<QueryParameterDescriptor> queryParameters,
        String description,
        String query,
        JdbcIO.RowMapper<T> rowMapper) {
      return queryParameters.apply(
          description,
          JdbcIO.<QueryParameterDescriptor, T>readAll()
              .withDataSourceConfiguration(dataSourceConfig)
              .withParameterSetter(
                  (JdbcIO.PreparedStatementSetter<QueryParameterDescriptor>)
                      (element, preparedStatement) -> {
                        preparedStatement.setString(1, element.resourceType());
                        preparedStatement.setInt(2, element.numBatches());
                        preparedStatement.setInt(3, element.batchId());
                      })
              // We are disabling this parameter because by default, this parameter causes
              // JdbcIO to
              // add a reshuffle transform after reading from the database. This breaks fusion
              // between the read and write operations, thus resulting in high memory overhead.
              // Disabling the below parameter results in optimal performance.
              .withOutputParallelization(false)
              .withQuery(query)
              .withRowMapper(rowMapper));
    }

    private PCollection<HapiRowDescriptor> joinResourceTagCollections(
        PCollection<HapiRowDescriptor> hapiRowDescriptorPCollection,
        PCollection<ResourceTag> resourceTagPCollection,
        PCollection<MdmLink> mdmLinkPCollection,
        PCollection<SourceIdentifier> sourceIdentifierPCollection) {
      // Step to convert HapiRowDescriptor to key value struct <ResourceId, HapiRowDescriptor>,
      // this will help in joining with tag PCollection.
      PCollection<KV<String, HapiRowDescriptor>> resourceCollection =
          hapiRowDescriptorPCollection.apply(
              "Convert Resource Collection to KV",
              ParDo.of(
                  new DoFn<HapiRowDescriptor, KV<String, HapiRowDescriptor>>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                      HapiRowDescriptor hapiRowDescriptor = processContext.element();
                      processContext.output(
                          KV.of(hapiRowDescriptor.resourceId(), hapiRowDescriptor));
                    }
                  }));

      // Step to convert ResourceTag to key value struct <ResourceId, ResourceTag>,
      // this will help in joining with resource PCollection.
      PCollection<KV<String, ResourceTag>> tagCollection =
          resourceTagPCollection.apply(
              "Convert Tag Collection to KV",
              ParDo.of(
                  new DoFn<ResourceTag, KV<String, ResourceTag>>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                      ResourceTag resourceTag = processContext.element();
                      processContext.output(KV.of(resourceTag.getResourceId(), resourceTag));
                    }
                  }));

      // Helper Tuples for joining.
      TupleTag<HapiRowDescriptor> resourceTuple = new TupleTag<>();
      TupleTag<ResourceTag> resourceTagTuple = new TupleTag<>();
      TupleTag<MdmLink> mdmLinkTuple = new TupleTag<>();
      TupleTag<SourceIdentifier> sourceIdentifierTuple = new TupleTag<>();

      // Join the collections.
      KeyedPCollectionTuple<String> partialJoinedResourceCollection =
          KeyedPCollectionTuple.of(resourceTuple, resourceCollection)
              .and(resourceTagTuple, tagCollection);

      if (mapToGoldenResources) {
        PCollection<KV<String, MdmLink>> mdmLinkCollection =
            mdmLinkPCollection.apply(
                "Convert MdmLink Collection to KV",
                ParDo.of(
                    new DoFn<MdmLink, KV<String, MdmLink>>() {
                      @ProcessElement
                      public void processElement(ProcessContext processContext) {
                        MdmLink mdmLink = processContext.element();
                        processContext.output(KV.of(mdmLink.getResourceId(), mdmLink));
                      }
                    }));
        partialJoinedResourceCollection =
            partialJoinedResourceCollection.and(mdmLinkTuple, mdmLinkCollection);
      }

      if (isMdmResource) {
        PCollection<KV<String, SourceIdentifier>> sourceIdentifierCollection =
            sourceIdentifierPCollection.apply(
                "Convert SourceIdentifier Collection to KV",
                ParDo.of(
                    new DoFn<SourceIdentifier, KV<String, SourceIdentifier>>() {
                      @ProcessElement
                      public void processElement(ProcessContext processContext) {
                        SourceIdentifier sourceIdentifier = processContext.element();
                        processContext.output(
                            KV.of(sourceIdentifier.getResourceId(), sourceIdentifier));
                      }
                    }));
        partialJoinedResourceCollection =
            partialJoinedResourceCollection.and(sourceIdentifierTuple, sourceIdentifierCollection);
      }

      PCollection<KV<String, CoGbkResult>> joinedResourceCollection =
          partialJoinedResourceCollection.apply(CoGroupByKey.create());

      // Process the HapiRowDescriptor collection from the joined collection.
      return joinedResourceCollection.apply(
          "Join Resource, Tag, MDM Link, and Source Identifier Collections",
          ParDo.of(
              new DoFn<KV<String, CoGbkResult>, HapiRowDescriptor>() {
                @ProcessElement
                public void process(ProcessContext processContext) {
                  KV<String, CoGbkResult> element = processContext.element();
                  Iterable<HapiRowDescriptor> hapiRowDescriptorIterable =
                      element.getValue().getAll(resourceTuple);
                  Iterable<ResourceTag> resourceTagIterable =
                      element.getValue().getAll(resourceTagTuple);

                  List<ResourceTag> tags = new ArrayList<>();
                  for (ResourceTag resourceTag : resourceTagIterable) {
                    tags.add(resourceTag);
                  }

                  List<MdmLink> mdmLinks = new ArrayList<>();
                  if (mapToGoldenResources) {
                    Iterable<MdmLink> mdmLinkIterable = element.getValue().getAll(mdmLinkTuple);
                    for (MdmLink mdmLink : mdmLinkIterable) {
                      mdmLinks.add(mdmLink);
                    }
                  }

                  List<SourceIdentifier> sourceIdentifiers = new ArrayList<>();
                  if (isMdmResource) {
                    Iterable<SourceIdentifier> sourceIdentifierIterable =
                        element.getValue().getAll(sourceIdentifierTuple);
                    for (SourceIdentifier sourceIdentifier : sourceIdentifierIterable) {
                      sourceIdentifiers.add(sourceIdentifier);
                    }
                  }

                  Iterator<HapiRowDescriptor> iterator = hapiRowDescriptorIterable.iterator();
                  if (iterator.hasNext()) {
                    HapiRowDescriptor hapiRowDescriptor = iterator.next();
                    // This is to avoid IllegalMutationException.
                    HapiRowDescriptor rowDescriptor = SerializationUtils.clone(hapiRowDescriptor);
                    rowDescriptor.setTags(tags);
                    rowDescriptor.setMdmLinks(mdmLinks);
                    rowDescriptor.setSourceIdentifiers(sourceIdentifiers);
                    processContext.output(rowDescriptor);
                  }
                }
              }));
    }
  }

  /**
   * Generates the query parameters for the JdbcIO fetch. The query parameters are generated based
   * on the batch size option set for the job.
   *
   * @param options the pipeline options
   * @param resourceType the resource type
   * @param numResources total number of resources of the given type
   * @return a list of query parameters in the form of lists of strings
   */
  @VisibleForTesting
  List<QueryParameterDescriptor> generateQueryParameters(
      FhirEtlOptions options, String resourceType, int numResources) {
    log.info("Generating query parameters for " + resourceType);

    int jdbcFetchSize = options.getJdbcFetchSize();
    List<QueryParameterDescriptor> queryParameterList = new ArrayList<QueryParameterDescriptor>();
    int numBatches = numResources / jdbcFetchSize;
    if (numResources % jdbcFetchSize != 0) {
      numBatches += 1;
    }
    log.info(
        "Generating query parameters for resource type {}; numResources= {} numBatches= {} ",
        resourceType,
        numResources,
        numBatches);

    for (int i = 0; i < numBatches; i++) {
      queryParameterList.add(QueryParameterDescriptor.create(resourceType, numBatches, i));
    }

    return queryParameterList;
  }

  /**
   * Searches for the total number of resources for each resource type
   *
   * @param resourceList the resource types to be processed
   * @param since the time from which the records need to be fetched
   * @return a Map storing the counts of each resource type
   */
  public Map<String, Integer> searchResourceCounts(
      String resourceList, String since, String mdmResourceList) throws SQLException {
    Set<String> resourceTypes = new HashSet<>(Arrays.asList(resourceList.split(",")));
    Map<String, Integer> resourceCountMap = new HashMap<>();
    for (String resourceType : resourceTypes) {
      StringBuilder builder = new StringBuilder();
      builder.append("SELECT count(*) as count FROM hfj_resource res");
      if (mdmResourceList.contains(resourceType)) {
        builder.append(
            " LEFT JOIN hfj_res_tag tag ON tag.res_id = res.res_id JOIN hfj_tag_def tag_def"
                + " ON tag_def.tag_id = tag.tag_id"
                + " WHERE tag_def.tag_code = 'GOLDEN_RECORD' AND res.res_type = ?");
      } else {
        builder.append(" WHERE res.res_type = ?");
      }
      if (Strings.isNullOrEmpty(since)) { // full mode
        builder.append(" AND res.res_deleted_at IS NULL ");
      } else { // incremental mode
        builder.append(" AND res.res_updated > '").append(since).append("'");
      }
      try (Connection connection = jdbcSource.getConnection();
          PreparedStatement statement =
              createPreparedStatement(connection, builder.toString(), resourceType);
          ResultSet resultSet = statement.executeQuery()) {
        resultSet.next();
        int count = resultSet.getInt("count");
        resourceCountMap.put(resourceType, count);
        log.info("Number of {} resources in DB = {}", resourceType, count);
      }
    }
    return resourceCountMap;
  }

  private PreparedStatement createPreparedStatement(
      Connection connection, String query, String parameter) throws SQLException {
    PreparedStatement preparedStatement = connection.prepareStatement(query);
    preparedStatement.setString(1, parameter);
    return preparedStatement;
  }
}
