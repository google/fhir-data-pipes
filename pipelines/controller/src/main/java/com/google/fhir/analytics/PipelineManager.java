/*
 * Copyright 2020-2025 Google LLC
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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.fhir.analytics.metrics.CumulativeMetrics;
import com.google.fhir.analytics.metrics.PipelineMetrics;
import com.google.fhir.analytics.metrics.PipelineMetricsProvider;
import com.google.fhir.analytics.model.DatabaseConfiguration;
import com.google.fhir.analytics.view.ViewDefinitionException;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.Data;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

/**
 * This is the class responsible for managing everything pipeline related. It is supposed to be a
 * singleton (and thread-safe), hence the private constructor and builder method. We rely on Spring
 * for the singleton constraint as it is the default (instead of a private constructor and a static
 * instance which does not play with Spring's dependency injection).
 */
@EnableScheduling
@Component
// The fields in this class are Spring managed via the @Autowired and/or @PostConstruct annotation
// method (hence these suppress annotations).
@SuppressWarnings("NullAway.Init")
public class PipelineManager implements ApplicationListener<ApplicationReadyEvent> {

  private static final Logger logger = LoggerFactory.getLogger(PipelineManager.class.getName());

  // The unused suppression is to avoid warnings for the fields which are injected by Spring, see
  // @PostConstruct annotation method initDwhStatus()
  @SuppressWarnings("unused")
  @Autowired
  private DataProperties dataProperties;

  @SuppressWarnings("unused")
  @Autowired
  private DwhFilesManager dwhFilesManager;

  @SuppressWarnings("unused")
  @Autowired
  private MeterRegistry meterRegistry;

  private HiveTableManager hiveTableManager;

  private PipelineThread currentPipeline;

  @Nullable private DwhFiles currentDwh;

  @Nullable private LocalDateTime lastRunEnd;

  private CronExpression cron;

  private DwhRunDetails lastRunDetails;

  private FlinkConfiguration flinkConfiguration;

  private AvroConversionUtil avroConversionUtil;

  private static final String ERROR_FILE_NAME = "error.log";
  private static final String SUCCESS = "SUCCESS";
  private static final String FAILURE = "FAILURE";
  private static final String FAILED_TO_START = "FAILED_TO_START";

  static enum RunMode {
    INCREMENTAL,
    FULL,
    VIEWS,
  }

  /**
   * This method publishes the beam pipeline metrics to the spring boot actuator. Previous metrics
   * are removed from the actuator before publishing the latest metrics
   *
   * @param metricResults The pipeline metrics that needs to be pushed
   */
  void publishPipelineMetrics(MetricResults metricResults) {
    MetricQueryResults metricQueryResults = EtlUtils.getMetrics(metricResults);
    for (MetricResult<Long> metricCounterResult : metricQueryResults.getCounters()) {
      Gauge.builder(
              metricCounterResult.getName().getNamespace()
                  + "_"
                  + metricCounterResult.getName().getName(),
              () -> metricCounterResult.getAttempted())
          // Make a strong reference so that the value is not immediately cleared after GC
          .strongReference(true)
          .register(meterRegistry);
    }
    for (MetricResult<GaugeResult> metricGaugeResult : metricQueryResults.getGauges()) {
      Gauge.builder(
              metricGaugeResult.getName().getNamespace()
                  + "_"
                  + metricGaugeResult.getName().getName(),
              () -> metricGaugeResult.getAttempted().getValue())
          .strongReference(true)
          .register(meterRegistry);
    }
  }

  void removePipelineMetrics() {
    meterRegistry
        .getMeters()
        .forEach(
            meter -> {
              if (meter.getId().getName().startsWith(MetricsConstants.METRICS_NAMESPACE)) {
                meterRegistry.remove(meter);
              }
            });
  }

  @Nullable
  public CumulativeMetrics getCumulativeMetrics() {
    // TODO Generate metrics and stats even for incremental and recreate views run; incremental run
    //  has two pipelines running one after the other, come up with a strategy to aggregate the
    //  metrics and generate the stats.
    if (isBatchRun() && isRunning()) {
      PipelineMetrics pipelineMetrics =
          PipelineMetricsProvider.getPipelineMetrics(currentPipeline.pipelineRunnerClass);
      return pipelineMetrics != null ? pipelineMetrics.getCumulativeMetricsForOngoingBatch() : null;
    }
    return null;
  }

  private void setLastRunStatus(LastRunStatus status) {
    if (status == LastRunStatus.SUCCESS) {
      lastRunEnd = DwhFilesManager.getCurrentTime();
    }
  }

  @PostConstruct
  private void initDwhStatus() throws ProfileException {

    // Initialise the Flink configurations for all the pipelines
    initialiseFlinkConfiguration();
    avroConversionUtil =
        AvroConversionUtil.getInstance(
            dataProperties.getFhirVersion(),
            dataProperties.getStructureDefinitionsPath(),
            dataProperties.getRecursiveDepth());

    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    FileSystems.setDefaultPipelineOptions(pipelineConfig.getFhirEtlOptions());
    validateFhirSourceConfiguration(pipelineConfig.getFhirEtlOptions());

    cron = CronExpression.parse(dataProperties.getIncrementalSchedule());
    String rootPrefix = dataProperties.getDwhRootPrefix();
    Preconditions.checkState(rootPrefix != null && !rootPrefix.isEmpty());

    String lastCompletedDwh = "";
    String lastDwh = "";
    String baseDir = dwhFilesManager.getBaseDir(rootPrefix);
    try {
      String prefix = dwhFilesManager.getPrefix(rootPrefix);
      List<ResourceId> paths =
          DwhFiles.getAllChildDirectories(baseDir).stream()
              .filter(dir -> dir.getFilename() != null && dir.getFilename().startsWith(prefix))
              .collect(Collectors.toList());

      String pathFileName;
      for (ResourceId path : paths) {
        pathFileName = path.getFilename();

        if (pathFileName == null) continue;

        if (!pathFileName.startsWith(prefix + DwhFiles.TIMESTAMP_PREFIX)) {
          // This is not necessarily an error; the user may want to bootstrap from an already
          // created DWH outside the control-panel framework, e.g., by running the batch pipeline
          // directly.
          logger.warn(
              "DWH directory {} does not start with {}{}",
              paths,
              prefix,
              DwhFiles.TIMESTAMP_PREFIX);
        }

        if (lastDwh.isEmpty() || lastDwh.compareTo(pathFileName) < 0) {
          logger.debug("Found a more recent DWH {}", pathFileName);
          lastDwh = pathFileName;
        }

        // Do not consider if the DWH is not completely created earlier.
        if (!dwhFilesManager.isDwhComplete(path)) {
          continue;
        }

        if (lastCompletedDwh.isEmpty() || lastCompletedDwh.compareTo(pathFileName) < 0) {
          logger.debug("Found a more recent completed DWH {}", pathFileName);
          lastCompletedDwh = pathFileName;
        }
      }
    } catch (IOException e) {
      logger.error("IOException while initializing DWH: ", e);
      throw new RuntimeException(e);
    }
    if (lastCompletedDwh.isEmpty()) {
      logger.info("No DWH found; it should be created by running a full pipeline");
      currentDwh = null;
      lastRunEnd = null;
    } else {
      logger.info("Initializing with most recent DWH {}", lastCompletedDwh);
      try {
        ResourceId resourceId =
            FileSystems.matchNewResource(baseDir, true)
                .resolve(lastCompletedDwh, StandardResolveOptions.RESOLVE_DIRECTORY);
        String currentDwhRoot = resourceId.toString();
        // TODO: If there are errors from the last VIEW run, expose them in the UI.
        currentDwh =
            DwhFiles.forRootWithLatestViewPath(currentDwhRoot, avroConversionUtil.getFhirContext());
        // There exists a DWH from before, so we set the scheduler to continue updating the DWH.
        lastRunEnd = DwhFilesManager.getCurrentTime();
      } catch (IOException e) {
        logger.error("IOException while initializing DWH: ", e);
        throw new RuntimeException(e);
      }
    }

    if (!lastDwh.isEmpty()) {
      initialiseLastRunDetails(baseDir, lastDwh);
    }
  }

  FhirContext getFhirContext() {
    return avroConversionUtil.getFhirContext();
  }

  private void initialiseFlinkConfiguration() {
    flinkConfiguration = new FlinkConfiguration();
    try {
      flinkConfiguration.initialiseFlinkConfiguration(dataProperties);
    } catch (IOException e) {
      logger.error("IOException while initializing Flink Configuration: ", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method initialises the lastRunDetails based on the dwh snapshot created recently. In case
   * an incremental run exists in the snapshot, the status is set based on the incremental run.
   */
  private void initialiseLastRunDetails(String baseDir, String dwhDirectory) {
    try {
      ResourceId dwhDirectoryPath =
          FileSystems.matchNewResource(baseDir, true)
              .resolve(dwhDirectory, StandardResolveOptions.RESOLVE_DIRECTORY);
      ResourceId incPath = DwhFiles.getLatestIncrementalRunPath(dwhDirectoryPath.toString());
      if (incPath != null) {
        updateLastRunDetails(incPath);
        return;
      }
      // In case of no incremental run, the status is set based on the dwhDirectory snapshot.
      updateLastRunDetails(dwhDirectoryPath);
    } catch (IOException e) {
      logger.error("IOException while initialising last run details ", e);
      throw new RuntimeException(e);
    }
  }

  private void updateLastRunDetails(ResourceId dwhDirectoryPath) throws IOException {
    if (dwhFilesManager.isDwhComplete(dwhDirectoryPath)) {
      setLastRunDetails(dwhDirectoryPath.toString(), SUCCESS);
    } else if (dwhFilesManager.isDwhJobStarted(dwhDirectoryPath)) {
      setLastRunDetails(dwhDirectoryPath.toString(), FAILURE);
    } else {
      setLastRunDetails(dwhDirectoryPath.toString(), FAILED_TO_START);
    }
  }

  /**
   * Validate the FHIR source configuration parameters during the launch of the application. This is
   * to detect any mis-configurations earlier enough and avoid failures during pipeline runs.
   */
  void validateFhirSourceConfiguration(FhirEtlOptions options) {
    if (Boolean.TRUE.equals(options.isJdbcModeHapi())) {
      validateDbConfigParameters(options.getFhirDatabaseConfigPath());
    } else if (!Strings.isNullOrEmpty(options.getFhirServerUrl())) {
      validateFhirServerParams(options);
    }
  }

  private void validateDbConfigParameters(String dbConfigPath) {
    try {
      DatabaseConfiguration dbConfiguration =
          DatabaseConfiguration.createConfigFromFile(dbConfigPath);
      boolean isValid =
          JdbcConnectionUtil.validateJdbcDetails(
              dbConfiguration.getJdbcDriverClass(),
              dbConfiguration.makeJdbsUrlFromConfig(),
              dbConfiguration.getDatabaseUser(),
              dbConfiguration.getDatabasePassword());
      Preconditions.checkArgument(isValid);
    } catch (IOException | SQLException | ClassNotFoundException e) {
      logger.error("Error occurred while validating jdbc details", e);
      throw new RuntimeException(e);
    }
  }

  private void validateFhirServerParams(FhirEtlOptions options) {
    FetchUtil fetchUtil =
        new FetchUtil(
            options.getFhirServerUrl(),
            options.getFhirServerUserName(),
            options.getFhirServerPassword(),
            options.getFhirServerOAuthTokenEndpoint(),
            options.getFhirServerOAuthClientId(),
            options.getFhirServerOAuthClientSecret(),
            options.getCheckPatientEndpoint(),
            avroConversionUtil.getFhirContext());
    fetchUtil.testFhirConnection();
  }

  synchronized boolean isBatchRun() {
    return (currentPipeline != null) && (currentPipeline.runMode == RunMode.FULL);
  }

  synchronized boolean isRunning() {
    return currentPipeline != null && currentPipeline.isAlive();
  }

  synchronized String getCurrentDwhRoot() {
    if (currentDwh == null) {
      return "";
    }
    return currentDwh.getRoot();
  }

  /**
   * Fetches the next scheduled time to run the incremental pipeline.
   *
   * @return the next scheduled time to run the incremental pipeline or null iff a pipeline is
   *     currently running or no previous DWH exist.
   */
  @Nullable LocalDateTime getNextIncrementalTime() {
    if (isRunning() || lastRunEnd == null) {
      return null;
    }
    return cron.next(lastRunEnd);
  }

  DwhRunDetails getLastRunDetails() {
    return lastRunDetails;
  }

  // Every 30 seconds, check for pipeline status and incremental pipeline schedule.
  @SuppressWarnings("unused")
  @Scheduled(fixedDelay = 30000)
  private void checkSchedule() throws IOException {
    LocalDateTime next = getNextIncrementalTime();
    if (next == null) {
      return;
    }
    logger.info("Last run was at {} next run is at {}", lastRunEnd, next);
    if (next.compareTo(DwhFilesManager.getCurrentTime()) < 0) {
      logger.info("Incremental run triggered at {}", DwhFilesManager.getCurrentTime());
      runIncrementalPipeline();
    }
  }

  synchronized void runBatchPipeline(boolean isRecreateViews) {
    Preconditions.checkState(!isRunning(), "cannot start a pipeline while another one is running");
    Preconditions.checkState(
        !Strings.isNullOrEmpty(getCurrentDwhRoot()) || !isRecreateViews,
        "cannot recreate views because no DWH is found!");
    PipelineConfig pipelineConfig =
        (isRecreateViews
            ? dataProperties.createRecreateViewsOptions(getCurrentDwhRoot())
            : dataProperties.createBatchOptions());
    FhirEtlOptions options = pipelineConfig.getFhirEtlOptions();
    FlinkPipelineOptions flinkOptions = options.as(FlinkPipelineOptions.class);
    if (!Strings.isNullOrEmpty(flinkConfiguration.getFlinkConfDir())) {
      flinkOptions.setFlinkConfDir(flinkConfiguration.getFlinkConfDir());
    }
    // sanity check; should always pass!
    FhirEtl.validateOptions(options);

    currentPipeline =
        new PipelineThread(
            options,
            this,
            pipelineConfig,
            isRecreateViews ? RunMode.VIEWS : RunMode.FULL,
            avroConversionUtil,
            FlinkRunner.class);
    if (isRecreateViews) {
      logger.info(
          "Running pipeline for recreating views from DWH {}", options.getParquetInputDwhRoot());
    } else {
      logger.info("Running full pipeline for DWH {}", options.getOutputParquetPath());
    }
    // We will only have one thread for running pipelines hence no need for a thread pool.
    currentPipeline.start();
  }

  synchronized void runIncrementalPipeline() throws IOException {
    // TODO do the same as above but read/set --since
    Preconditions.checkState(!isRunning(), "cannot start a pipeline while another one is running");
    Preconditions.checkState(
        currentDwh != null,
        "cannot start the incremental pipeline while there are no DWHs; run full pipeline");
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    FhirEtlOptions options = pipelineConfig.getFhirEtlOptions();
    String finalDwhRoot = options.getOutputParquetPath();
    String incrementalDwhRoot = currentDwh.newIncrementalRunPath().toString();
    options.setOutputParquetPath(incrementalDwhRoot);
    String incrementalViewPath = DwhFiles.newViewsPath(incrementalDwhRoot).toString();
    if (dataProperties.isCreateParquetViews()) {
      options.setOutputParquetViewPath(incrementalViewPath);
    }
    String since = fetchSinceTimestamp(options);
    options.setSince(since);
    FlinkPipelineOptions flinkOptionsForBatch = options.as(FlinkPipelineOptions.class);
    if (!Strings.isNullOrEmpty(flinkConfiguration.getFlinkConfDir())) {
      flinkOptionsForBatch.setFlinkConfDir(flinkConfiguration.getFlinkConfDir());
    }
    // sanity check; should always pass!
    FhirEtl.validateOptions(options);

    // The merger pipeline merges the original full DWH with the new incremental one.
    ParquetMergerOptions mergerOptions = PipelineOptionsFactory.as(ParquetMergerOptions.class);
    mergerOptions.setDwh1(currentDwh.getRoot());
    mergerOptions.setDwh2(incrementalDwhRoot);
    mergerOptions.setMergedDwh(finalDwhRoot);
    mergerOptions.setRunner(FlinkRunner.class);
    mergerOptions.setViewDefinitionsDir(options.getViewDefinitionsDir());
    // The number of shards is set based on the parallelism available for the FlinkRunner
    // Pipeline
    // TODO: For Flink non-local mode, refactor this to be not dependent on the
    // pipeline-controller
    //  machine https://github.com/google/fhir-data-pipes/issues/893
    int numShards =
        dataProperties.getNumThreads() > 0
            ? dataProperties.getNumThreads()
            : Runtime.getRuntime().availableProcessors();
    mergerOptions.setNumShards(numShards);
    mergerOptions.setStructureDefinitionsPath(
        Strings.nullToEmpty(dataProperties.getStructureDefinitionsPath()));
    mergerOptions.setFhirVersion(dataProperties.getFhirVersion());
    if (dataProperties.getRowGroupSizeForParquetFiles() > 0) {
      mergerOptions.setRowGroupSizeForParquetFiles(dataProperties.getRowGroupSizeForParquetFiles());
    }
    FlinkPipelineOptions flinkOptionsForMerge = mergerOptions.as(FlinkPipelineOptions.class);
    if (!Strings.isNullOrEmpty(flinkConfiguration.getFlinkConfDir())) {
      flinkOptionsForMerge.setFlinkConfDir(flinkConfiguration.getFlinkConfDir());
    }
    flinkOptionsForMerge.setFasterCopy(true);
    if (dataProperties.getNumThreads() > 0) {
      flinkOptionsForMerge.setParallelism(dataProperties.getNumThreads());
    }
    // Creating a thread for running both pipelines, one after the other.
    currentPipeline =
        new PipelineThread(
            options, mergerOptions, this, pipelineConfig, avroConversionUtil, FlinkRunner.class);
    logger.info("Running incremental pipeline for DWH {} since {}", currentDwh.getRoot(), since);
    currentPipeline.start();
  }

  private String fetchSinceTimestamp(FhirEtlOptions options) throws IOException {
    Instant timestamp = null;
    if (FhirFetchMode.BULK_EXPORT.equals(options.getFhirFetchMode())) {
      try {
        timestamp =
            currentDwh != null
                ? currentDwh.readTimestampFile(DwhFiles.TIMESTAMP_FILE_BULK_TRANSACTION_TIME)
                : null;
      } catch (NoSuchFileException e) {
        logger.warn(
            "No bulk export timestamp file found for the previous run, will try to rely on the"
                + " start timestamp");
      }
    }

    if (timestamp == null) {
      timestamp =
          currentDwh != null ? currentDwh.readTimestampFile(DwhFiles.TIMESTAMP_FILE_START) : null;
    }
    if (timestamp == null) {
      throw new IllegalStateException(
          "Timestamp value is empty and hence cannot start the pipeline");
    }
    return timestamp.toString();
  }

  @Override
  public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
    if (!dataProperties.isCreateHiveResourceTables()) {
      logger.info("createHiveResourceTables is false; skipping Hive table creation.");
      return;
    }
    DatabaseConfiguration dbConfig;
    try {
      dbConfig =
          DatabaseConfiguration.createConfigFromFile(dataProperties.getThriftserverHiveConfig());
    } catch (IOException e) {
      logger.error("Exception while reading thrift hive config.");
      throw new RuntimeException(e);
    }
    try {
      hiveTableManager = new HiveTableManager(dbConfig, dataProperties.getHiveResourceViewsDir());
      hiveTableManager.showTables();
    } catch (SQLException e) {
      logger.error("Exception while querying the thriftserver: ", e);
      throw new RuntimeException(e);
    }

    // Making sure that the tables for the current DWH files are created.
    createResourceTables();
  }

  /**
   * This method, upon Controller start, checks on Thrift sever to create resource tables if they
   * don't exist. There is a @PostConstruct method present in this class which is initDwhStatus and
   * the reason below code has not been added because dataProperties.getThriftserverHiveConfig()
   * turns out to be null there.
   */
  public void createResourceTables() {
    if (!dataProperties.isCreateHiveResourceTables()) {
      logger.info("createHiveResourceTables is false; skipping Hive table creation.");
      return;
    }
    String rootPrefix = dataProperties.getDwhRootPrefix();
    Preconditions.checkState(rootPrefix != null && !rootPrefix.isEmpty());

    String prefix = dwhFilesManager.getPrefix(rootPrefix);
    String baseDir = dwhFilesManager.getBaseDir(rootPrefix);

    try {
      List<ResourceId> paths =
          DwhFiles.getAllChildDirectories(baseDir).stream()
              .filter(
                  dir ->
                      dir.getFilename() != null
                          && dir.getFilename().startsWith(prefix + DwhFiles.TIMESTAMP_PREFIX))
              .filter(
                  dir -> {
                    try {
                      return dwhFilesManager.isDwhComplete(dir);
                    } catch (IOException e) {
                      logger.error("Error while accessing {}", dir, e);
                    }
                    return false;
                  })
              .collect(Collectors.toList());

      // Sort snapshots directories such that the canonical view is created for the latest one.
      paths.sort(Comparator.comparing(ResourceId::toString));

      // TODO: Why are we creating these tables for all paths and not just the most recent? If all
      //  are needed, why are we doing the above `sort`?
      for (ResourceId path : paths) {
        if (path.getFilename() != null) {
          String timestamp = getTimestampSuffix(path.getFilename());
          if (timestamp != null) {
            logger.info("Creating resource tables for relative path {}", path.getFilename());
            String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(rootPrefix);
            String pathDwhRoot = baseDir + fileSeparator + path.getFilename();
            createHiveTablesIfNeeded(pathDwhRoot, timestamp, path.getFilename());
          }
        } else {
          logger.warn("Skipping path {} as it does not have a valid filename", path);
        }
      }
    } catch (IOException e) {
      // In case of exceptions at this stage, we just log the exception.
      logger.error("Exception while reading thriftserver parquet output directory: ", e);
    } catch (ViewDefinitionException e) {
      logger.error("Exception while reading ViewDefinitions: ", e);
    }
  }

  @Nullable
  private String getTimestampSuffix(String path) {
    List<String> tokens = Splitter.on(DwhFiles.TIMESTAMP_PREFIX).splitToList(path);
    if (tokens.isEmpty()) return null;
    return tokens.get(tokens.size() - 1).replaceAll("/", "");
  }

  private void createHiveTablesIfNeeded(
      String dwhRoot, String timestampSuffix, String thriftServerParquetPath)
      throws IOException, ViewDefinitionException {
    if (!dataProperties.isCreateHiveResourceTables()) return;

    List<String> existingResources = dwhFilesManager.findExistingResources(dwhRoot);

    try {
      logger.info("Creating resources on Hive server for resources: {}", existingResources);
      hiveTableManager.createResourceAndCanonicalTables(
          existingResources, timestampSuffix, thriftServerParquetPath, true);
      if (dataProperties.isCreateParquetViews()) {
        ResourceId viewRoot = DwhFiles.getLatestViewsPath(dwhRoot);
        // TODO a more complete approach is to fallback to the latest complete view set.
        if (viewRoot != null && dwhFilesManager.isDwhComplete(viewRoot)) {
          List<String> existingViews =
              dwhFilesManager.findExistingViews(
                  viewRoot.toString(), dataProperties.getViewDefinitionsDir());
          String sep = DwhFiles.getFileSeparatorForDwhFiles(dataProperties.getDwhRootPrefix());
          String thriftServerViewPath = thriftServerParquetPath + sep + viewRoot.getFilename();
          // This is to differentiate view-sets where we have multiple view-sets per DWH root.
          String viewTimestampSuffix = timestampSuffix;
          String viewTimestamp =
              viewRoot.getFilename() != null ? getTimestampSuffix(viewRoot.getFilename()) : null;
          if (viewTimestamp != null) {
            viewTimestampSuffix = timestampSuffix + "_" + viewTimestamp;
          }
          hiveTableManager.createResourceAndCanonicalTables(
              existingViews, viewTimestampSuffix, thriftServerViewPath, false);
        }
      }
      logger.info("Created resources on Thrift server Hive");
    } catch (SQLException e) {
      logger.error(
          "Exception while creating resource table on thriftserver for path: {}", dwhRoot, e);
    }
  }

  private synchronized void updateDwh(String newRoot) throws IOException {
    currentDwh = DwhFiles.forRootWithLatestViewPath(newRoot, avroConversionUtil.getFhirContext());
  }

  private static class PipelineThread extends Thread {
    private FhirEtlOptions options;
    private final PipelineManager manager;
    // This is used in the incremental mode only.
    @Nullable private final ParquetMergerOptions mergerOptions;

    private final PipelineConfig pipelineConfig;

    @Nullable private final RunMode runMode;

    private AvroConversionUtil avroConversionUtil;

    private Class<? extends PipelineRunner> pipelineRunnerClass;

    PipelineThread(
        FhirEtlOptions options,
        PipelineManager manager,
        PipelineConfig pipelineConfig,
        @Nullable RunMode runMode,
        AvroConversionUtil avroConversionUtil,
        Class<? extends PipelineRunner> pipelineRunnerClass) {
      Preconditions.checkArgument(options != null);
      this.options = options;
      this.manager = manager;
      this.pipelineConfig = pipelineConfig;
      this.runMode = runMode;
      this.mergerOptions = null;
      this.avroConversionUtil = avroConversionUtil;
      this.pipelineRunnerClass = pipelineRunnerClass;
    }

    // The constructor for the incremental pipeline (hence the `mergerOptions`).
    PipelineThread(
        FhirEtlOptions options,
        ParquetMergerOptions mergerOptions,
        PipelineManager manager,
        PipelineConfig pipelineConfig,
        AvroConversionUtil avroConversionUtil,
        Class<? extends PipelineRunner> pipelineRunnerClass) {
      Preconditions.checkArgument(options != null);
      this.options = options;
      this.manager = manager;
      this.mergerOptions = mergerOptions;
      this.pipelineConfig = pipelineConfig;
      this.avroConversionUtil = avroConversionUtil;
      this.pipelineRunnerClass = pipelineRunnerClass;
      this.runMode = RunMode.INCREMENTAL;
    }

    @Override
    public void run() {
      long start = System.currentTimeMillis();
      // The number of threads may increase after a few runs, but it should eventually go down.
      logger.info(
          "Pipelines execution started with a new thread; number of threads is {}",
          Thread.activeCount());
      String currentDwhRoot = null;
      try {
        if (runMode != RunMode.VIEWS) {
          currentDwhRoot = options.getOutputParquetPath();
        } else {
          currentDwhRoot = options.getParquetInputDwhRoot();
        }

        List<Pipeline> pipelines = FhirEtl.setupAndBuildPipelines(options, avroConversionUtil);
        if (pipelines == null || pipelines.isEmpty()) {
          logger.warn("No resources found to be fetched!");
          manager.setLastRunStatus(LastRunStatus.SUCCESS);
          return;
        }

        List<PipelineResult> pipelineResults =
            EtlUtils.runMultiplePipelinesWithTimestamp(pipelines, options);
        // Remove the metrics of the previous pipeline and register the new metrics
        manager.removePipelineMetrics();
        pipelineResults.forEach(
            pipelineResult -> manager.publishPipelineMetrics(pipelineResult.metrics()));
        if (runMode == RunMode.VIEWS) {
          // Nothing more is needed to be done as we do not recreate a new DWH in this mode.
          // TODO record timing info and other details in this case.
          return;
        }
        if (mergerOptions == null) { // Do not update DWH yet if this was an incremental run.
          manager.updateDwh(currentDwhRoot);
        } else {
          currentDwhRoot = mergerOptions.getMergedDwh();
          List<Pipeline> mergerPipelines =
              ParquetMerger.createMergerPipelines(mergerOptions, avroConversionUtil);
          logger.info("Merger options are {}", mergerOptions);
          List<PipelineResult> mergerPipelineResults =
              EtlUtils.runMultipleMergerPipelinesWithTimestamp(mergerPipelines, mergerOptions);
          mergerPipelineResults.forEach(
              pipelineResult -> manager.publishPipelineMetrics(pipelineResult.metrics()));
          manager.updateDwh(currentDwhRoot);
        }
        manager.createHiveTablesIfNeeded(
            currentDwhRoot,
            pipelineConfig.getTimestampSuffix(),
            pipelineConfig.getThriftServerParquetPath());
        manager.setLastRunStatus(LastRunStatus.SUCCESS);
        manager.setLastRunDetails(currentDwhRoot, SUCCESS);
      } catch (Exception e) {
        logger.error("exception while running pipeline: ", e);
        manager.captureError(currentDwhRoot, e);
        manager.setLastRunDetails(currentDwhRoot, FAILURE);
        manager.setLastRunStatus(LastRunStatus.FAILURE);
      } finally {
        // See https://github.com/google/fhir-data-pipes/issues/777#issuecomment-1703142297
        System.gc();
        logger.info(
            "Total time taken for the pipelines = {} secs",
            (System.currentTimeMillis() - start) / 1000);
      }
    }
  }

  /** This method captures the given exception into a file rooted at the dwhRoot location. */
  void captureError(@Nullable String dwhRoot, Exception e) {
    try {
      if (!Strings.isNullOrEmpty(dwhRoot)) {
        String stackTrace = ExceptionUtils.getStackTrace(e);
        DwhFiles.overwriteFile(
            dwhRoot, ERROR_FILE_NAME, stackTrace.getBytes(StandardCharsets.UTF_8));
      } else {
        logger.warn("DWH root is null or empty; cannot capture error");
      }
    } catch (IOException ex) {
      logger.error("Error while capturing error to a file", ex);
    }
  }

  /** Sets the details of the last pipeline run with the given dwhRoot as the snapshot location. */
  void setLastRunDetails(@Nullable String dwhRoot, String status) {
    if (!Strings.isNullOrEmpty(dwhRoot)) {
      // TODO for `status`, use an enum instead of String.
      DwhRunDetails dwhRunDetails = new DwhRunDetails();
      try {
        if (!FAILED_TO_START.equalsIgnoreCase(status)) {
          String startTime =
              DwhFiles.readTimestampFile(dwhRoot, DwhFiles.TIMESTAMP_FILE_START).toString();
          dwhRunDetails.setStartTime(startTime);
        }
        if (SUCCESS.equalsIgnoreCase(status)) {
          String endTime =
              DwhFiles.readTimestampFile(dwhRoot, DwhFiles.TIMESTAMP_FILE_END).toString();
          dwhRunDetails.setEndTime(endTime);
        } else {
          String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(dwhRoot);
          dwhRoot = dwhRoot.endsWith(fileSeparator) ? dwhRoot : dwhRoot + fileSeparator;
          ResourceId errorResource = FileSystems.matchNewResource(dwhRoot + ERROR_FILE_NAME, false);
          if (dwhFilesManager.doesFileExist(errorResource)) {
            dwhRunDetails.setErrorLogPath(dwhRoot + ERROR_FILE_NAME);
          }
        }
        dwhRunDetails.setStatus(status);
        this.lastRunDetails = dwhRunDetails;
      } catch (IOException e) {
        logger.error("Error while updating last run details", e);
        throw new RuntimeException(e);
      }
    } else {
      logger.warn("DWH root is null or empty; cannot set last run details");
    }
  }

  public enum LastRunStatus {
    NOT_RUN,
    SUCCESS,
    FAILURE
  }

  @Data
  public static class DwhRunDetails {

    private String startTime;
    private String endTime;
    private String status;
    private String errorLogPath;
  }
}
