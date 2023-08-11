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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.ParserOptions;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.FhirContexts;
import com.google.common.annotations.VisibleForTesting;
import com.google.fhir.analytics.JdbcConnectionPools.DataSourceConfig;
import com.google.fhir.analytics.model.DatabaseConfiguration;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;
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
 * done during `setup()` where PipelienOptions not available) we use inheritance.
 *
 * @param <T> The type of the elements of the input PCollection.
 */
abstract class FetchSearchPageFn<T> extends DoFn<T, KV<String, Integer>> {

  private static final Logger log = LoggerFactory.getLogger(FetchSearchPageFn.class);

  private final Counter numFetchedResources;

  private final Counter totalFetchTimeMillis;

  private final Counter totalGenerateTimeMillis;

  private final Counter totalPushTimeMillis;

  private final String sourceUrl;

  private final String sourceUser;

  private final String sourcePw;

  protected final String sinkPath;

  private final String sinkUsername;

  private final String sinkPassword;

  protected final String stageIdentifier;

  protected final String parquetFile;

  private final int secondsToFlush;

  private final int rowGroupSize;

  protected final DataSourceConfig sinkDbConfig;

  private final String sinkDbTableName;

  private final boolean useSingleSinkDbTable;

  private final int initialPoolSize;

  private final int maxPoolSize;

  @VisibleForTesting protected ParquetUtil parquetUtil;

  protected OpenmrsUtil openmrsUtil;

  protected FhirSearchUtil fhirSearchUtil;

  protected FhirStoreUtil fhirStoreUtil;

  protected JdbcResourceWriter jdbcWriter;

  protected IParser parser;

  protected FhirContext fhirContext;

  FetchSearchPageFn(FhirEtlOptions options, String stageIdentifier) {
    this.sinkPath = options.getFhirSinkPath();
    this.sinkUsername = options.getSinkUserName();
    this.sinkPassword = options.getSinkPassword();
    this.sourceUrl = options.getFhirServerUrl();
    this.sourceUser = options.getFhirServerUserName();
    this.sourcePw = options.getFhirServerPassword();
    this.stageIdentifier = stageIdentifier;
    this.parquetFile = options.getOutputParquetPath();
    this.secondsToFlush = options.getSecondsToFlushParquetFiles();
    this.rowGroupSize = options.getRowGroupSizeForParquetFiles();
    this.sinkDbTableName = options.getSinkDbTablePrefix();
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
    this.useSingleSinkDbTable = options.getUseSingleSinkTable();
    this.initialPoolSize = options.getJdbcInitialPoolSize();
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
  public void setup() throws SQLException, PropertyVetoException {
    log.debug("Starting setup for stage " + stageIdentifier);
    // TODO make this configurable
    //   https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/400
    fhirContext = FhirContexts.forR4();
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
    openmrsUtil = new OpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
    fhirSearchUtil = new FhirSearchUtil(openmrsUtil);
    parquetUtil =
        new ParquetUtil(
            fhirContext.getVersion().getVersion(),
            parquetFile,
            secondsToFlush,
            rowGroupSize,
            stageIdentifier + "_");
    if (sinkDbConfig != null) {
      DataSource jdbcSink =
          JdbcConnectionPools.getInstance()
              .getPooledDataSource(sinkDbConfig, initialPoolSize, maxPoolSize);
      jdbcWriter =
          new JdbcResourceWriter(jdbcSink, sinkDbTableName, useSingleSinkDbTable, fhirContext);
    }
  }

  @Teardown
  public void teardown() throws IOException {
    parquetUtil.closeAllWriters();
  }

  protected void addFetchTime(long millis) {
    totalFetchTimeMillis.inc(millis);
  }

  protected void processBundle(Bundle bundle) throws IOException, SQLException {
    this.processBundle(bundle, null);
  }

  // TODO remove `resourceTypes` once we support different FHIR versions in AVRO conversion.
  protected void processBundle(Bundle bundle, @Nullable Set<String> resourceTypes)
      throws IOException, SQLException {
    if (bundle != null && bundle.getEntry() != null) {
      numFetchedResources.inc(bundle.getEntry().size());
      if (!parquetFile.isEmpty()) {
        long startTime = System.currentTimeMillis();
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
        for (BundleEntryComponent entry : bundle.getEntry()) {
          Resource resource = entry.getResource();
          if (resourceTypes != null && resourceTypes.contains(resource.getResourceType().name())) {
            jdbcWriter.writeResource(resource);
          }
        }
      }
    }
  }
}
