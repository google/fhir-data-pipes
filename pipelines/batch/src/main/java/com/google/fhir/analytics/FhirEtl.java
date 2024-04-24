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

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.fhir.analytics.metrics.PipelineMetrics;
import com.google.fhir.analytics.metrics.PipelineMetricsProvider;
import com.google.fhir.analytics.model.DatabaseConfiguration;
import com.google.fhir.analytics.view.ViewDefinitionException;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Beam pipeline for reading FHIR resources from OpenMRS and pushing them into a data warehouse.
 */
public class FhirEtl {

  private static final Logger log = LoggerFactory.getLogger(FhirEtl.class);

  /**
   * This map is used when the activePeriod feature is enabled in the JDBC mode. For each table it
   * indicates the column on which the date filter is applied. It is best if these columns are not
   * nullable and there is an index on them.
   */
  private static final Map<String, String> tableDateColumn;

  static {
    Map<String, String> tempMap = Maps.newHashMap();
    tempMap.put("encounter", "encounter_datetime");
    tempMap.put("obs", "obs_datetime");
    tempMap.put("visit", "date_started");
    tableDateColumn = Collections.unmodifiableMap(tempMap);
  }

  static FhirSearchUtil createFhirSearchUtil(FhirEtlOptions options, FhirContext fhirContext) {
    return new FhirSearchUtil(createFetchUtil(options, fhirContext));
  }

  static FetchUtil createFetchUtil(FhirEtlOptions options, FhirContext fhirContext) {
    return new FetchUtil(
        options.getFhirServerUrl(),
        options.getFhirServerUserName(),
        options.getFhirServerPassword(),
        options.getFhirServerOAuthTokenEndpoint(),
        options.getFhirServerOAuthClientId(),
        options.getFhirServerOAuthClientSecret(),
        fhirContext);
  }

  /**
   * For each SearchSegmentDescriptor, it fetches the given resources, convert them to output
   * Parquet/JSON files, and output the IDs of the fetched resources.
   *
   * @param inputSegments each element defines a set of resources to be fetched in one FHIR call.
   * @param resourceType the type of the resources, e.g., Patient or Observation
   * @param options the pipeline options
   * @return a PCollection of all patient IDs of fetched resources or empty if `resourceType` has no
   *     patient ID association.
   */
  private static PCollection<KV<String, Integer>> fetchSegmentsAndReturnPatientIds(
      PCollection<SearchSegmentDescriptor> inputSegments,
      String resourceType,
      FhirEtlOptions options) {
    FetchResources fetchResources = new FetchResources(options, resourceType + "_main");
    return inputSegments.apply(fetchResources);
  }

  static void fetchPatientHistory(
      Pipeline pipeline,
      List<PCollection<KV<String, Integer>>> allPatientIds,
      Set<String> patientAssociatedResources,
      FhirEtlOptions options,
      AvroConversionUtil avroConversionUtil)
      throws ProfileException {
    PCollectionList<KV<String, Integer>> patientIdList =
        PCollectionList.<KV<String, Integer>>empty(pipeline).and(allPatientIds);
    PCollection<KV<String, Integer>> flattenedPatients =
        patientIdList.apply(Flatten.pCollections());
    PCollection<KV<String, Integer>> mergedPatients = flattenedPatients.apply(Sum.integersPerKey());
    final String patientType = "Patient";
    FetchPatients fetchPatients =
        new FetchPatients(options, avroConversionUtil.getResourceSchema(patientType));
    mergedPatients.apply(fetchPatients);
    for (String resourceType : patientAssociatedResources) {
      FetchPatientHistory fetchPatientHistory = new FetchPatientHistory(options, resourceType);
      mergedPatients.apply(fetchPatientHistory);
    }
  }

  private static List<Pipeline> buildFhirSearchPipeline(
      FhirEtlOptions options, AvroConversionUtil avroConversionUtil) throws ProfileException {
    FhirSearchUtil fhirSearchUtil =
        createFhirSearchUtil(options, avroConversionUtil.getFhirContext());
    Map<String, List<SearchSegmentDescriptor>> segmentMap = Maps.newHashMap();
    try {
      // TODO in the activePeriod case, among patientAssociatedResources, only fetch Encounter here.
      // TODO Capture the total resources to be processed as a metric which can be used to derive
      //  the stats of how many records has been completed.
      segmentMap = fhirSearchUtil.createSegments(options);
    } catch (IllegalArgumentException e) {
      log.error(
          "Either the date format in the active period is wrong or none of the resources support"
              + " 'date' feature"
              + e.getMessage());
      throw e;
    }
    if (segmentMap.isEmpty()) {
      return null;
    }

    Pipeline pipeline = Pipeline.create(options);
    List<PCollection<KV<String, Integer>>> allPatientIds = Lists.newArrayList();
    for (Map.Entry<String, List<SearchSegmentDescriptor>> entry : segmentMap.entrySet()) {
      String resourceType = entry.getKey();
      PCollection<SearchSegmentDescriptor> inputSegments =
          pipeline.apply(Create.of(entry.getValue()));
      allPatientIds.add(fetchSegmentsAndReturnPatientIds(inputSegments, resourceType, options));
    }
    if (!options.getActivePeriod().isEmpty()) {
      Set<String> patientAssociatedResources =
          fhirSearchUtil.findPatientAssociatedResources(segmentMap.keySet());
      fetchPatientHistory(
          pipeline, allPatientIds, patientAssociatedResources, options, avroConversionUtil);
    }
    return Arrays.asList(pipeline);
  }

  private static DataSource createJdbcPooledDataSource(
      FhirEtlOptions options, DatabaseConfiguration dbConfig) {
    if (options.getJdbcInitialPoolSize() != JdbcConnectionPools.MIN_CONNECTIONS) {
      log.warn("Setting jdbcInitialPoolSize has no effect; it is deprecated and will be removed.");
    }
    return JdbcConnectionPools.getInstance()
        .getPooledDataSource(
            JdbcConnectionPools.dbConfigToDataSourceConfig(dbConfig), options.getJdbcMaxPoolSize());
  }

  private static List<Pipeline> buildOpenmrsJdbcPipeline(
      FhirEtlOptions options, AvroConversionUtil avroConversionUtil)
      throws PropertyVetoException, IOException, SQLException, ProfileException {
    // TODO add incremental support.
    Preconditions.checkArgument(Strings.isNullOrEmpty(options.getSince()));
    FhirSearchUtil fhirSearchUtil =
        createFhirSearchUtil(options, avroConversionUtil.getFhirContext());
    List<Pipeline> pipelines = new ArrayList<>();
    Pipeline pipeline = Pipeline.create(options);
    pipelines.add(pipeline);
    DatabaseConfiguration dbConfig =
        DatabaseConfiguration.createConfigFromFile(options.getFhirDatabaseConfigPath());
    DataSource jdbcSource = createJdbcPooledDataSource(options, dbConfig);
    JdbcFetchOpenMrs jdbcUtil = new JdbcFetchOpenMrs(jdbcSource);
    int batchSize =
        Math.min(
            options.getBatchSize(), 170); // batch size > 200 will result in HTTP 400 Bad Request
    Map<String, List<String>> reverseMap =
        jdbcUtil.createFhirReverseMap(options.getResourceList(), dbConfig);
    // process each table-resource mappings
    Set<String> resourceTypes = new HashSet<>();
    List<PCollection<KV<String, Integer>>> allPatientIds = Lists.newArrayList();
    for (Map.Entry<String, List<String>> entry : reverseMap.entrySet()) {
      String tableName = entry.getKey();
      log.info(String.format("List of resources for table %s is %s", tableName, entry.getValue()));
      PCollection<String> uuids;
      if (options.getActivePeriod().isEmpty() || !tableDateColumn.containsKey(tableName)) {
        if (!options.getActivePeriod().isEmpty()) {
          log.warn(
              String.format(
                  "There is no date mapping for table %s; fetching all rows.", tableName));
        }
        uuids = jdbcUtil.fetchAllUuids(pipeline, tableName, options.getJdbcFetchSize());
      } else {
        try {
          uuids =
              jdbcUtil.fetchUuidsByDate(
                  pipeline, tableName, tableDateColumn.get(tableName), options.getActivePeriod());
        } catch (CannotProvideCoderException e) {
          // This should never happen!
          String error = "Cannot provide coder for String! " + e.getMessage();
          log.error("{} {}", error, e);
          throw new IllegalStateException(error);
        }
      }
      for (String resourceType : entry.getValue()) {
        resourceTypes.add(resourceType);
        String baseBundleUrl = options.getFhirServerUrl() + "/" + resourceType;
        PCollection<SearchSegmentDescriptor> inputSegments =
            uuids.apply(
                String.format("CreateSearchSegments_%s_table_%s", resourceType, tableName),
                new JdbcFetchOpenMrs.CreateSearchSegments(resourceType, baseBundleUrl, batchSize));
        allPatientIds.add(fetchSegmentsAndReturnPatientIds(inputSegments, resourceType, options));
      }
    }
    if (!options.getActivePeriod().isEmpty()) {
      Set<String> patientAssociatedResources =
          fhirSearchUtil.findPatientAssociatedResources(resourceTypes);
      fetchPatientHistory(
          pipeline, allPatientIds, patientAssociatedResources, options, avroConversionUtil);
    }
    return pipelines;
  }

  static void validateOptions(FhirEtlOptions options) {
    if (!options.getActivePeriod().isEmpty()) {
      Set<String> resourceSet = Sets.newHashSet(options.getResourceList().split(","));
      if (resourceSet.contains("Patient")) {
        throw new IllegalArgumentException(
            "When using --activePeriod feature, 'Patient' should not be in --resourceList got: "
                + options.getResourceList());
      }
      if (!resourceSet.contains("Encounter")) {
        throw new IllegalArgumentException(
            "When using --activePeriod feature, 'Encounter' should be in --resourceList got: "
                + options.getResourceList());
      }
    }

    if (!options.getParquetInputDwhRoot().isEmpty()
        || !options.getSourceJsonFilePattern().isEmpty()
        || !options.getSourceNDJsonFilePattern().isEmpty()) {

      if (!checkOnlyOneNonEmptyString(
          options.getParquetInputDwhRoot(),
          options.getSourceJsonFilePattern(),
          options.getSourceNDJsonFilePattern())) {
        throw new IllegalArgumentException(
            "Only one of the parameters --parquetInputDwhRoot, --sourceJsonFilePattern or"
                + " --sourceNDJsonFilePattern can be set at a time");
      }
      if (!options.getParquetInputDwhRoot().isEmpty()
          && !options.getOutputParquetPath().isEmpty()) {
        // This constraint is to make the PipelineManager logic simpler and because there is
        // currently no use-case for both reading from Parquet files and also writing into Parquet.
        throw new IllegalArgumentException(
            "--parquetInputDwhRoot and --outputParquetPath cannot be used together!");
      }
      if (!options.getFhirServerUrl().isEmpty()) {
        throw new IllegalArgumentException(
            "When reading from input files, --fhirServerUrl should not be set!");
      }
      if (options.isJdbcModeEnabled()) {
        throw new IllegalArgumentException(
            "When reading from input files, --jdbcModeEnabled should not be set!");
      }
      if (!options.getActivePeriod().isEmpty()) {
        throw new IllegalArgumentException(
            "Enabling --activePeriod is not supported when reading from input files");
      }
    } else { // sourceJsonFilePattern and parquetInputDwh are not set.
      if (options.getFhirServerUrl().isEmpty() && !options.isJdbcModeHapi()) {
        throw new IllegalArgumentException(
            "One of --fhirServerUrl --jdbcModeHapi --parquetInputDwhRoot --sourceJsonFilePattern"
                + " --sourceNDJsonFilePattern should be set!");
      }
    }
  }

  private static boolean checkOnlyOneNonEmptyString(String... strings) {
    List<String> nonEmptyList =
        Arrays.stream(strings)
            .filter(string -> !Strings.isNullOrEmpty(string))
            .collect(Collectors.toList());
    return nonEmptyList.size() == 1;
  }

  // TODO: Implement active period feature for JDBC mode with a HAPI source server (issue #278).
  private static List<Pipeline> buildHapiJdbcPipeline(FhirEtlOptions options)
      throws PropertyVetoException, SQLException, IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(options.getFhirDatabaseConfigPath()));
    DatabaseConfiguration dbConfig =
        DatabaseConfiguration.createConfigFromFile(options.getFhirDatabaseConfigPath());
    boolean foundResource = false;
    DataSource jdbcSource = createJdbcPooledDataSource(options, dbConfig);

    JdbcFetchHapi jdbcFetchHapi = new JdbcFetchHapi(jdbcSource);
    Map<String, Integer> resourceCount =
        jdbcFetchHapi.searchResourceCounts(options.getResourceList(), options.getSince());

    List<Pipeline> pipelines = new ArrayList<>();
    long totalNumOfResources = 0l;
    for (String resourceType : options.getResourceList().split(",")) {
      int numResources = resourceCount.get(resourceType);
      if (numResources == 0) {
        continue;
      }

      totalNumOfResources += numResources;
      foundResource = true;
      Pipeline pipeline = Pipeline.create(options);
      PCollection<QueryParameterDescriptor> queryParameters =
          pipeline.apply(
              "Generate query parameters for " + resourceType,
              Create.of(
                  new JdbcFetchHapi(jdbcSource)
                      .generateQueryParameters(options, resourceType, numResources)));
      PCollection<HapiRowDescriptor> payload =
          queryParameters.apply(
              "JdbcIO fetch for " + resourceType,
              new JdbcFetchHapi.FetchRowsJdbcIo(
                  options.getResourceList(),
                  JdbcIO.DataSourceConfiguration.create(jdbcSource),
                  options.getSince()));

      payload.apply(
          "Convert to parquet for " + resourceType,
          ParDo.of(new ConvertResourceFn(options, "ConvertResourceFn")));
      pipelines.add(pipeline);
    }

    if (foundResource) { // Otherwise, there is nothing to be done!
      PipelineMetrics pipelineMetrics =
          PipelineMetricsProvider.getPipelineMetrics(options.getRunner());
      if (pipelineMetrics != null) {
        pipelineMetrics.clearAllMetrics();
        pipelineMetrics.setTotalNumOfResources(totalNumOfResources);
      }
      return pipelines;
    }
    return null;
  }

  private static List<Pipeline> buildParquetReadPipeline(
      FhirEtlOptions options, AvroConversionUtil avroConversionUtil)
      throws IOException, ProfileException {
    Preconditions.checkArgument(!options.getParquetInputDwhRoot().isEmpty());
    DwhFiles dwhFiles =
        DwhFiles.forRoot(options.getParquetInputDwhRoot(), avroConversionUtil.getFhirContext());
    Set<String> resourceTypes = dwhFiles.findNonEmptyFhirResourceTypes();
    log.info("Reading Parquet files for these resource types: {}", resourceTypes);
    List<Pipeline> pipelineList = new ArrayList<>();
    for (String resourceType : resourceTypes) {
      Pipeline pipeline = Pipeline.create(options);
      PCollection<ReadableFile> inputFiles =
          pipeline
              .apply(Create.of(dwhFiles.getFilePattern(resourceType)))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches());

      PCollection<GenericRecord> records =
          inputFiles.apply(ParquetIO.readFiles(avroConversionUtil.getResourceSchema(resourceType)));

      records.apply(
          "Process Parquet records for " + resourceType,
          ParDo.of(new ProcessGenericRecords(options, resourceType)));
      pipelineList.add(pipeline);
    }
    return pipelineList;
  }

  private static List<Pipeline> buildJsonReadPipeline(
      FhirEtlOptions options, String filePattern, boolean isFileNDJson) {
    Preconditions.checkArgument(!filePattern.isEmpty());
    Preconditions.checkArgument(Strings.isNullOrEmpty(options.getSince()));
    Preconditions.checkArgument(!options.isJdbcModeEnabled());
    Preconditions.checkArgument(options.getActivePeriod().isEmpty());

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(FileIO.match().filepattern(filePattern))
        .apply(FileIO.readMatches())
        .apply(
            isFileNDJson ? "Read NDJson Files" : "Read JSON Files",
            ParDo.of(new ReadJsonFilesFn(options, isFileNDJson)));
    return Arrays.asList(pipeline);
  }

  /**
   * Pipeline builder for fetching resources from a FHIR server. The mode of the pipeline is decided
   * based on the given `options`. There are currently four modes in this priority order:
   *
   * <p>1) Fetching directly from the HAPI database if `options.isJdbcModeHapi()` is set.
   *
   * <p>2) A combination of direct DB and OpenMRS FHIR API access if `options.isJdbcModeEnabled()`.
   *
   * <p>3) Reading from input JSON files if `options.getSourceJsonFilePattern()` is set.
   *
   * <p>4) Using FHIR search API if none of the above options are set.
   *
   * <p>Depending on the options, the created pipeline may write into Parquet files, push resources
   * to another FHIR server, write into another database, etc.
   *
   * @param options the pipeline options to be used.
   * @return the created Pipeline instance or null if nothing needs to be done.
   */
  static List<Pipeline> setupAndBuildPipelines(
      FhirEtlOptions options, AvroConversionUtil avroConversionUtil)
      throws PropertyVetoException, IOException, SQLException, ViewDefinitionException,
          ProfileException {
    if (!options.getSinkDbConfigPath().isEmpty()) {
      JdbcResourceWriter.createTables(options);
    }
    if (options.isJdbcModeHapi()) {
      return buildHapiJdbcPipeline(options);
    } else if (options.isJdbcModeEnabled()) {
      return buildOpenmrsJdbcPipeline(options, avroConversionUtil);
    } else if (!options.getParquetInputDwhRoot().isEmpty()) {
      return buildParquetReadPipeline(options, avroConversionUtil);
    } else if (!options.getSourceJsonFilePattern().isEmpty()) {
      return buildJsonReadPipeline(options, options.getSourceJsonFilePattern(), false);
    } else if (!options.getSourceNDJsonFilePattern().isEmpty()) {
      return buildJsonReadPipeline(options, options.getSourceNDJsonFilePattern(), true);
    } else {
      return buildFhirSearchPipeline(options, avroConversionUtil);
    }
  }

  public static void main(String[] args)
      throws PropertyVetoException, IOException, SQLException, ViewDefinitionException,
          ProfileException {

    AvroConversionUtil.initializeAvroConverters();

    PipelineOptionsFactory.register(FhirEtlOptions.class);
    FhirEtlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    log.info("Flags: " + options);
    validateOptions(options);
    AvroConversionUtil avroConversionUtil =
        AvroConversionUtil.getInstance(
            options.getFhirVersion(),
            options.getStructureDefinitionsPath(),
            options.getRecursiveDepth());
    List<Pipeline> pipelines = setupAndBuildPipelines(options, avroConversionUtil);
    EtlUtils.runMultiplePipelinesWithTimestamp(
        pipelines, options, avroConversionUtil.getFhirContext());

    log.info("DONE!");
  }
}
