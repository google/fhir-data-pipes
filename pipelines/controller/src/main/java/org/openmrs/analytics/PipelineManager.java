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

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.FhirContexts;
import com.google.common.base.Preconditions;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.openmrs.analytics.metrics.PipelineMetrics;
import org.openmrs.analytics.metrics.PipelineMetricsFactory;
import org.openmrs.analytics.model.DatabaseConfiguration;
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
public class PipelineManager implements ApplicationListener<ApplicationReadyEvent> {

  private static final Logger logger = LoggerFactory.getLogger(PipelineManager.class.getName());

  @Autowired private DataProperties dataProperties;

  @Autowired private PipelineMetricsFactory pipelineMetricsFactory;

  private PipelineThread currentPipeline;

  private DwhFiles currentDwh;

  private LocalDateTime lastRunEnd;

  private CronExpression cron;

  // TODO expose this in the web-UI
  private LastRunStatus lastRunStatus = LastRunStatus.NOT_RUN;

  public MetricQueryResults getMetricQueryResults() {
    // TODO Generate metrics and stats even for incremental run, incremental run has two pipelines
    //  running one after the other, come up with a strategy to aggregate the metrics and generate
    //  the stats
    if (isBatchRun() && isRunning()) {
      PipelineMetrics pipelineMetrics =
          pipelineMetricsFactory.getPipelineMetrics(
              currentPipeline.pipeline.getOptions().getRunner());
      return pipelineMetrics.getMetricQueryResults();
    }
    return null;
  }

  private void setLastRunStatus(LastRunStatus status) {
    lastRunStatus = status;
    if (status == LastRunStatus.SUCCESS) {
      lastRunEnd = LocalDateTime.now();
    }
  }

  @PostConstruct
  private void initDwhStatus() {

    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    FileSystems.setDefaultPipelineOptions(pipelineConfig.getFhirEtlOptions());

    cron = CronExpression.parse(dataProperties.getIncrementalSchedule());
    String rootPrefix = dataProperties.getDwhRootPrefix();
    Preconditions.checkState(rootPrefix != null && !rootPrefix.isEmpty());

    String lastDwh = "";
    String baseDir = getBaseDir(rootPrefix);
    try {
      String prefix = getPrefix(rootPrefix);
      List<ResourceId> paths =
          getAllChildDirectories(baseDir).stream()
              .filter(dir -> dir.getFilename().startsWith(prefix))
              .collect(Collectors.toList());

      Preconditions.checkState(paths != null, "Make sure DWH prefix is a valid path!");

      for (ResourceId path : paths) {
        if (!path.getFilename().startsWith(prefix + DataProperties.TIMESTAMP_PREFIX)) {
          // This is not necessarily an error; the user may want to bootstrap from an already
          // created DWH outside the control-panel framework, e.g., by running the batch pipeline
          // directly.
          logger.warn(
              "DWH directory {} does not start with {}",
              paths,
              prefix + DataProperties.TIMESTAMP_PREFIX);
        }
        if (lastDwh.isEmpty() || lastDwh.compareTo(path.getFilename()) < 0) {
          logger.debug("Found a more recent DWH {}", path.getFilename());
          lastDwh = path.getFilename();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (lastDwh.isEmpty()) {
      logger.info("No DWH found; it should be created by running a full pipeline");
      currentDwh = null;
      lastRunEnd = null;
    } else {
      logger.info("Initializing with most recent DWH {}", lastDwh);
      ResourceId resourceId =
          FileSystems.matchNewResource(baseDir, true)
              .resolve(lastDwh, StandardResolveOptions.RESOLVE_DIRECTORY);
      currentDwh = DwhFiles.forRoot(resourceId.toString());
      // There exists a DWH from before, so we set the scheduler to continue updating the DWH.
      lastRunEnd = LocalDateTime.now();
    }
  }

  private Set<ResourceId> getAllChildDirectories(String baseDir) throws IOException {
    ResourceId resourceId =
        FileSystems.matchNewResource(baseDir, true)
            .resolve("*/*", StandardResolveOptions.RESOLVE_FILE);
    List<MatchResult> matchResultList =
        FileSystems.matchResources(Collections.singletonList(resourceId));
    Set<ResourceId> childDirectories = new HashSet<>();
    for (MatchResult matchResult : matchResultList) {
      if (matchResult.status() == Status.OK && !matchResult.metadata().isEmpty()) {
        for (Metadata metadata : matchResult.metadata()) {
          childDirectories.add(metadata.resourceId().getCurrentDirectory());
        }
      } else if (matchResult.status() == Status.ERROR) {
        logger.error("Error matching resource types under {} ", baseDir);
        throw new IOException(String.format("Error matching resource types under %s", baseDir));
      }
    }
    logger.info("Child resources : {}", childDirectories);
    return childDirectories;
  }

  private String getBaseDir(String dwhRootPrefix) {
    int index = dwhRootPrefix.lastIndexOf("/");
    if (index <= 0) {
      String errorMessage =
          "dwhRootPrefix should be configured with a non-empty base directory. It should be of the"
              + " format <baseDir>/<prefix>";
      logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
    return dwhRootPrefix.substring(0, index);
  }

  private String getPrefix(String dwhRootPrefix) {
    int index = dwhRootPrefix.lastIndexOf("/");
    String prefix = dwhRootPrefix.substring(index + 1);
    if (prefix == null || prefix.isBlank()) {
      String errorMessage =
          "dwhRootPrefix should be configured with a non-empty suffix string after the last"
              + " occurrence of the character '/'";
      logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
    return prefix;
  }

  synchronized boolean isBatchRun() {
    return currentPipeline != null && currentPipeline.isBatchRun;
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
   * @return the next scheduled time to run the incremental pipeline or null iff a pipeline is
   *     currently running or no previous DWH exist.
   */
  LocalDateTime getNextIncrementalTime() {
    if (isRunning() || lastRunEnd == null) {
      return null;
    }
    return cron.next(lastRunEnd);
  }

  // Every 30 seconds, check for pipeline status and incremental pipeline schedule.
  @Scheduled(fixedDelay = 30000)
  private void checkSchedule() throws IOException, PropertyVetoException, SQLException {
    LocalDateTime next = getNextIncrementalTime();
    if (next == null) {
      return;
    }
    logger.info("Last run was at {} next run is at {}", lastRunEnd, next);
    if (next.compareTo(LocalDateTime.now()) < 0) {
      logger.info("Incremental run triggered at {}", LocalDateTime.now());
      runIncrementalPipeline();
    }
  }

  private Pipeline buildJdbcPipeline(FhirEtlOptions options)
      throws IOException, PropertyVetoException, SQLException {
    DatabaseConfiguration dbConfig =
        DatabaseConfiguration.createConfigFromFile(options.getFhirDatabaseConfigPath());
    FhirContext fhirContext = FhirContexts.forR4();
    logger.info("Creating HAPI JDBC pipeline with options {}", options);
    return FhirEtl.buildHapiJdbcFetch(options, dbConfig, fhirContext);
  }

  synchronized void runBatchPipeline() throws IOException, PropertyVetoException, SQLException {
    Preconditions.checkState(!isRunning(), "cannot start a pipeline while another one is running");
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    FhirEtlOptions options = pipelineConfig.getFhirEtlOptions();
    Pipeline pipeline = buildJdbcPipeline(options);
    if (pipeline == null) {
      logger.warn("No resources found to be fetched!");
      return;
    } else {
      currentPipeline = new PipelineThread(pipeline, this, dataProperties, pipelineConfig, true);
    }
    logger.info("Running full pipeline for DWH {}", options.getOutputParquetPath());
    // We will only have one thread for running pipelines hence no need for a thread pool.
    currentPipeline.start();
  }

  synchronized void runIncrementalPipeline()
      throws IOException, PropertyVetoException, SQLException {
    // TODO do the same as above but read/set --since
    Preconditions.checkState(!isRunning(), "cannot start a pipeline while another one is running");
    Preconditions.checkState(
        currentDwh != null,
        "cannot start the incremental pipeline while there are no DWHs; run full pipeline");
    PipelineConfig pipelineConfig = dataProperties.createBatchOptions();
    FhirEtlOptions options = pipelineConfig.getFhirEtlOptions();
    String finalDwhRoot = options.getOutputParquetPath();
    // TODO move old incremental_run dir if there is one
    String incrementalDwhRoot = currentDwh.newIncrementalRunPath().toString();
    options.setOutputParquetPath(incrementalDwhRoot);
    String since = currentDwh.readTimestampFile().toString();
    options.setSince(since);
    options.setProcessDeletedRecords(Boolean.TRUE);
    Pipeline pipeline = buildJdbcPipeline(options);

    // The merger pipeline merges the original full DWH with the new incremental one.
    ParquetMergerOptions mergerOptions = PipelineOptionsFactory.as(ParquetMergerOptions.class);
    mergerOptions.setDwh1(currentDwh.getRoot());
    mergerOptions.setDwh2(incrementalDwhRoot);
    mergerOptions.setMergedDwh(finalDwhRoot);
    mergerOptions.setRunner(FlinkRunner.class);
    mergerOptions.setNumShards(dataProperties.getMaxWorkers());
    FlinkPipelineOptions flinkOptions = mergerOptions.as(FlinkPipelineOptions.class);
    flinkOptions.setFasterCopy(true);
    flinkOptions.setMaxParallelism(dataProperties.getMaxWorkers());

    if (pipeline == null) {
      // TODO communicate this to the UI
      logger.info("No new resources to be fetched!");
      setLastRunStatus(LastRunStatus.SUCCESS);
    } else {
      // Creating a thread for running both pipelines, one after the other.
      currentPipeline =
          new PipelineThread(pipeline, mergerOptions, this, dataProperties, pipelineConfig, false);
      logger.info("Running incremental pipeline for DWH {} since {}", currentDwh.getRoot(), since);
      currentPipeline.start();
    }
  }

  /**
   * This method checks upon Controller start checks on Thrift sever to create resource tables if
   * they don't exist.
   */
  @Override
  public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
    if (!dataProperties.isCreateHiveResourceTables()) {
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
    HiveTableManager hiveTableManager =
        new HiveTableManager(
            dbConfig.makeJdbsUrlFromConfig(),
            dbConfig.getDatabaseUser(),
            dbConfig.getDatabasePassword());

    String rootPrefix = dataProperties.getDwhRootPrefix();
    Preconditions.checkState(rootPrefix != null && !rootPrefix.isEmpty());

    String prefix = getPrefix(rootPrefix);
    String baseDir = getBaseDir(rootPrefix);
    List<ResourceId> paths;
    try {
      paths =
          getAllChildDirectories(baseDir).stream()
              .filter(dir -> dir.getFilename().startsWith(prefix))
              .collect(Collectors.toList());

      Preconditions.checkState(paths != null, "Make sure DWH prefix is a valid path!");

      int snapshotCount = 0;
      for (ResourceId path : paths) {
        if (!path.getFilename().startsWith(prefix + DataProperties.TIMESTAMP_PREFIX)) {
          logger.warn(
              "DWH directory {} does not start with {}",
              paths,
              prefix + DataProperties.TIMESTAMP_PREFIX);
        }
        snapshotCount++;
        Set<ResourceId> childPaths = getAllChildDirectories(baseDir + "/" + path.getFilename());
        for (ResourceId resourceId : childPaths) {
          String resource = resourceId.getFilename();
          String[] tokens = path.getFilename().split(prefix + DataProperties.TIMESTAMP_PREFIX);
          if (tokens.length > 1) {
            String timestamp = tokens[1];
            String thriftServerParquetPath =
                baseDir + "/" + path.getFilename() + "/" + resourceId.getFilename();
            logger.debug("thriftServerParquetPath: ", thriftServerParquetPath);
            hiveTableManager.createResourceTable(resource, timestamp, thriftServerParquetPath);
            // Create Canonical table for the latest snapshot.
            if (snapshotCount == paths.size()) {
              hiveTableManager.createResourceCanonicalTable(resource, thriftServerParquetPath);
            }
          }
        }
      }
    } catch (IOException e) {
      logger.error("Exception while reading thriftserver parquet output directory.");
      throw new RuntimeException(e);
    } catch (SQLException e) {
      logger.error("Exception while creating resource tables on thriftserver.");
      throw new RuntimeException(e);
    }
  }

  private synchronized void updateDwh(String newRoot) {
    currentDwh = DwhFiles.forRoot(newRoot);
  }

  private static class PipelineThread extends Thread {

    private final Pipeline pipeline;
    private final PipelineManager manager;
    // This is used in the incremental mode only.
    private final ParquetMergerOptions mergerOptions;

    private final DataProperties dataProperties;

    private final PipelineConfig pipelineConfig;

    private final boolean isBatchRun;

    PipelineThread(
        Pipeline pipeline,
        PipelineManager manager,
        DataProperties dataProperties,
        PipelineConfig pipelineConfig,
        boolean isBatchRun) {
      Preconditions.checkArgument(pipeline.getOptions().as(FhirEtlOptions.class) != null);
      this.pipeline = pipeline;
      this.manager = manager;
      this.dataProperties = dataProperties;
      this.pipelineConfig = pipelineConfig;
      this.isBatchRun = isBatchRun;
      this.mergerOptions = null;
    }

    PipelineThread(
        Pipeline pipeline,
        ParquetMergerOptions mergerOptions,
        PipelineManager manager,
        DataProperties dataProperties,
        PipelineConfig pipelineConfig,
        boolean isBatchRun) {
      Preconditions.checkArgument(pipeline.getOptions().as(FhirEtlOptions.class) != null);
      this.pipeline = pipeline;
      this.manager = manager;
      this.mergerOptions = mergerOptions;
      this.dataProperties = dataProperties;
      this.pipelineConfig = pipelineConfig;
      this.isBatchRun = isBatchRun;
    }

    @Override
    public void run() {
      try {
        FhirEtlOptions options = pipeline.getOptions().as(FhirEtlOptions.class);
        EtlUtils.runPipelineWithTimestamp(pipeline, options);
        if (mergerOptions == null) { // Do not update DWH yet if this was an incremental run.
          manager.updateDwh(options.getOutputParquetPath());
        } else {
          FhirContext fhirContext = FhirContexts.forR4();
          Pipeline mergerPipeline = ParquetMerger.createMergerPipeline(mergerOptions, fhirContext);
          logger.info("Merger options are {}", mergerOptions);
          EtlUtils.runMergerPipelineWithTimestamp(mergerPipeline, mergerOptions);
          manager.updateDwh(mergerOptions.getMergedDwh());
        }
        if (dataProperties.isCreateHiveResourceTables()) {
          createHiveResourceTables(
              options.getResourceList(),
              pipelineConfig.getTimestampSuffix(),
              pipelineConfig.getThriftServerParquetPath());
        }
        manager.setLastRunStatus(LastRunStatus.SUCCESS);
      } catch (Exception e) {
        logger.error("exception while running pipeline: ", e);
        manager.setLastRunStatus(LastRunStatus.FAILURE);
      }
    }

    private void createHiveResourceTables(
        String resourceList, String timestampSuffix, String thriftServerParquetPath)
        throws IOException, SQLException {

      logger.info("Establishing connection to Thrift server Hive");
      DatabaseConfiguration dbConfig =
          DatabaseConfiguration.createConfigFromFile(dataProperties.getThriftserverHiveConfig());

      logger.info("Creating resources on Thrift server Hive");
      HiveTableManager hiveTableManager =
          new HiveTableManager(
              dbConfig.makeJdbsUrlFromConfig(),
              dbConfig.getDatabaseUser(),
              dbConfig.getDatabasePassword());
      hiveTableManager.createResourceTables(resourceList, timestampSuffix, thriftServerParquetPath);
      logger.info("Created resources on Thrift server Hive");
    }
  }

  public enum LastRunStatus {
    NOT_RUN,
    SUCCESS,
    FAILURE
  }
}
