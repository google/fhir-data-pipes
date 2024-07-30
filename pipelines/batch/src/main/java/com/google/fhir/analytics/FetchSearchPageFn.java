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
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.ParserOptions;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.fhir.analytics.JdbcConnectionPools.DataSourceConfig;
import com.google.fhir.analytics.model.DatabaseConfiguration;
import com.google.fhir.analytics.view.ViewApplicationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the common functionality for all Fns that need to fetch FHIR resources and convert them
 * to Avro and JSON records. The non-abstract sub-classes should implement `ProcessElement` using
 * `processBundle` auxiliary method. Note the code reuse pattern that we really need here is
 * composition (not inheritance) but because of Beam complexities (e.g., certain work need to be
 * done during `setup()` where PipelienOptions not available) we use inheritance. A better approach
 * is to create the utility instances (e.g., `fetchUtil`) once at the beginning of ParDo or
 * StartBundle method using a synchronized method. Those functions have acccess to PipelienOptions.
 * That way we can get rid of many instance variables that mirror PipelienOptions fields.
 *
 * @param <T> The type of the elements of the input PCollection.
 */
abstract class FetchSearchPageFn<T> extends DoFn<T, KV<String, Integer>> {

  private static final Logger log = LoggerFactory.getLogger(FetchSearchPageFn.class);

  private static final String DATAFLOW_RUNNER = "DataflowRunner";

  private final Counter numFetchedResources;

  private final Counter totalFetchTimeMillis;

  private final Counter totalGenerateTimeMillis;

  private final Counter totalPushTimeMillis;

  private final String sourceUrl;

  private final String sourceUser;

  private final String sourcePw;

  private final String oAuthTokenEndpoint;

  private final String oAuthClientId;

  private final String oAuthClientSecret;

  protected final String sinkPath;

  private final String sinkUsername;

  private final String sinkPassword;

  protected final String stageIdentifier;

  protected final String parquetFile;

  private final int secondsToFlush;

  private final int rowGroupSize;

  private final int recursiveDepth;

  protected final DataSourceConfig sinkDbConfig;

  protected final String viewDefinitionsDir;

  private final int maxPoolSize;

  @VisibleForTesting protected ParquetUtil parquetUtil;

  protected FetchUtil fetchUtil;

  protected FhirSearchUtil fhirSearchUtil;

  protected FhirStoreUtil fhirStoreUtil;

  protected JdbcResourceWriter jdbcWriter;

  protected IParser parser;

  private final FhirVersionEnum fhirVersionEnum;

  private final String structureDefinitionsPath;

  protected AvroConversionUtil avroConversionUtil;

  private final boolean createParquetViews;

  private final List<String> resourceList;

  FetchSearchPageFn(FhirEtlOptions options, String stageIdentifier) {
    this.resourceList = Arrays.asList(options.getResourceList().split(","));
    this.createParquetViews = options.isCreateParquetViews();
    this.sinkPath = options.getFhirSinkPath();
    this.sinkUsername = options.getSinkUserName();
    this.sinkPassword = options.getSinkPassword();
    this.sourceUrl = options.getFhirServerUrl();
    this.sourceUser = options.getFhirServerUserName();
    this.sourcePw = options.getFhirServerPassword();
    this.oAuthTokenEndpoint = options.getFhirServerOAuthTokenEndpoint();
    this.oAuthClientId = options.getFhirServerOAuthClientId();
    this.oAuthClientSecret = options.getFhirServerOAuthClientSecret();
    this.stageIdentifier = stageIdentifier;
    this.parquetFile = options.getOutputParquetPath();
    this.secondsToFlush = options.getSecondsToFlushParquetFiles();
    this.rowGroupSize = options.getRowGroupSizeForParquetFiles();
    this.viewDefinitionsDir = options.getViewDefinitionsDir();
    this.structureDefinitionsPath = options.getStructureDefinitionsPath();
    this.fhirVersionEnum = options.getFhirVersion();
    this.recursiveDepth = options.getRecursiveDepth();
    if (options.getSinkDbConfigPath().isEmpty()) {
      this.sinkDbConfig = null;
    } else {
      try {
        this.sinkDbConfig =
            JdbcConnectionPools.dbConfigToDataSourceConfig(
                DatabaseConfiguration.createConfigFromFile(options.getSinkDbConfigPath()));
      } catch (IOException e) {
        String error = "Cannot access file " + options.getSinkDbConfigPath();
        log.error(error);
        throw new IllegalArgumentException(error);
      }
    }
    this.maxPoolSize = options.getJdbcMaxPoolSize();
    this.numFetchedResources =
        Metrics.counter(
            MetricsConstants.METRICS_NAMESPACE,
            MetricsConstants.NUM_FETCHED_RESOURCES + stageIdentifier);
    this.totalFetchTimeMillis =
        Metrics.counter(
            MetricsConstants.METRICS_NAMESPACE,
            MetricsConstants.TOTAL_FETCH_TIME_MILLIS + stageIdentifier);
    this.totalGenerateTimeMillis =
        Metrics.counter(
            MetricsConstants.METRICS_NAMESPACE,
            MetricsConstants.TOTAL_GENERATE_TIME_MILLIS + stageIdentifier);
    this.totalPushTimeMillis =
        Metrics.counter(
            MetricsConstants.METRICS_NAMESPACE,
            MetricsConstants.TOTAL_PUSH_TIME_MILLIS + stageIdentifier);
  }

  @Setup
  public void setup() throws SQLException, ProfileException {
    log.debug("Starting setup for stage " + stageIdentifier);
    avroConversionUtil =
        AvroConversionUtil.getInstance(fhirVersionEnum, structureDefinitionsPath, recursiveDepth);
    FhirContext fhirContext = avroConversionUtil.getFhirContext();
    // The documentation for `FhirContext` claims that it is thread-safe but looking at the code,
    // it is not obvious if it is. This might be an issue when we write to it, like the next line.
    fhirContext.setParserOptions(
        // We want to keep the original IDs; this is particularly useful when the `fullUrl` is not
        // a URL but a URN, e.g., `urn:uuid:...`; for example when Bundles come from JSON files.
        // Note `IIdType.getIdPart` extracts only the logical ID part when exporting but that code
        // path does not work for URNs (e.g., when importing files).
        new ParserOptions().setOverrideResourceIdWithBundleEntryFullUrl(false));
    fhirContext.getRestfulClientFactory().setSocketTimeout(40000);
    // Note this parser is not used when fetching resources from a HAPI server. That's why we need
    // to change the `setOverrideResourceIdWithBundleEntryFullUrl` globally above such that the
    // parsers used in the HAPI client code is impacted too.
    parser = fhirContext.newJsonParser();
    fhirStoreUtil =
        FhirStoreUtil.createFhirStoreUtil(
            sinkPath, sinkUsername, sinkPassword, fhirContext.getRestfulClientFactory());
    fetchUtil =
        new FetchUtil(
            sourceUrl,
            sourceUser,
            sourcePw,
            oAuthTokenEndpoint,
            oAuthClientId,
            oAuthClientSecret,
            fhirContext);
    fhirSearchUtil = new FhirSearchUtil(fetchUtil);
    if (!Strings.isNullOrEmpty(parquetFile)) {
      parquetUtil =
          new ParquetUtil(
              fhirContext.getVersion().getVersion(),
              structureDefinitionsPath,
              parquetFile,
              viewDefinitionsDir,
              resourceList,
              secondsToFlush,
              rowGroupSize,
              stageIdentifier + "_",
              recursiveDepth,
              createParquetViews);
    }
    if (sinkDbConfig != null) {
      DataSource jdbcSink =
          JdbcConnectionPools.getInstance().getPooledDataSource(sinkDbConfig, maxPoolSize);
      // TODO separate view generation from writing; TBD in a more generic version of:
      //  https://github.com/google/fhir-data-pipes/issues/288
      jdbcWriter = new JdbcResourceWriter(jdbcSink, viewDefinitionsDir, fhirContext);
    }
  }

  /**
   * Note this is a hacky solution to address a DataflowRunner specific issue where @Teardown method
   * is not guaranteed to be called. Closing/flushing Parquet files in @FinishBundle is not a good
   * idea in general because it makes the size of those files a function of how Beam divides the
   * work into Bundles. That's why we only do this on DataflowRunner which tend to have very large
   * Bundles.
   *
   * <p>This should be overridden by all subclasses because the `context` type is not fully
   * specified at this parent class (because of the T type argument). All subclass implementations
   * should call `super.finishBundle` though. TODO: implement a way to enforce this at compile time;
   * this is currently caught at run time.
   */
  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    if (DATAFLOW_RUNNER.equals(context.getPipelineOptions().getRunner().getSimpleName())) {
      try {
        if (parquetUtil != null) {
          parquetUtil.flushAll();
        }
      } catch (IOException | ProfileException e) {
        // There is not much that we can do at finishBundle so just throw a RuntimeException
        log.error("At finishBundle caught exception ", e);
        throw new IllegalStateException(e);
      }
    }
  }

  @Teardown
  public void teardown() throws IOException {
    // Note this is _not_ guaranteed to be called; for example when the worker process is being
    // stopped, the runner may choose not to call teardown; currently this only happens for
    // DataflowRunner and that's why we have the finishBundle method above:
    // https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.Teardown.html
    if (parquetUtil != null) {
      parquetUtil.closeAllWriters();
    }
  }

  protected void addFetchTime(long millis) {
    totalFetchTimeMillis.inc(millis);
  }

  protected void processBundle(Bundle bundle)
      throws IOException, SQLException, ViewApplicationException, ProfileException {
    this.processBundle(bundle, null);
  }

  protected void processBundle(Bundle bundle, @Nullable Set<String> resourceTypes)
      throws IOException, SQLException, ViewApplicationException, ProfileException {
    if (bundle != null && bundle.getEntry() != null) {
      numFetchedResources.inc(bundle.getEntry().size());
      if (parquetUtil != null) {
        long startTime = System.currentTimeMillis();
        // TODO: The right way for writing Parquet records is to cache them and wait
        //  until processing a Beam Bundle is done, i.e., write in @FinishBundle.
        //  Otherwise, if a Beam Bundle fails in the middle, we will get duplicate records.
        //  This may also apply to when we write into a sink DB or another FHIR-server (below),
        //  unless if we take care of duplicate writes (which is for example the case when we
        //  apply ViewDefinition to a resource and delete rows with the same `id` first).
        parquetUtil.writeRecords(bundle, resourceTypes);
        totalGenerateTimeMillis.inc(System.currentTimeMillis() - startTime);
      }
      if (!this.sinkPath.isEmpty()) {
        long pushStartTime = System.currentTimeMillis();
        fhirStoreUtil.uploadBundle(bundle);
        totalPushTimeMillis.inc(System.currentTimeMillis() - pushStartTime);
      }
      if (sinkDbConfig != null) {
        if (bundle.getEntry() == null) {
          return;
        }
        // TODO consider processing the whole Bundle in one batched DB update.
        for (BundleEntryComponent entry : bundle.getEntry()) {
          Resource resource = entry.getResource();
          if (resourceTypes == null || resourceTypes.contains(resource.getResourceType().name())) {
            jdbcWriter.writeResource(resource);
          }
        }
      }
    }
  }
}
