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
import com.google.common.base.Preconditions;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import javax.annotation.PostConstruct;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.openmrs.analytics.model.DatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
public class PipelineManager {
  private static final Logger logger = LoggerFactory.getLogger(PipelineManager.class.getName());

  @Autowired private DataProperties dataProperties;

  private PipelineThread currentPipeline;

  private DwhFiles currentDwh;

  private LocalDateTime lastRunEnd;

  private CronExpression cron;

  // TODO expose this in the web-UI
  private LastRunStatus lastRunStatus = LastRunStatus.NOT_RUN;

  private void setLastRunStatus(LastRunStatus status) {
    lastRunStatus = status;
    if (status == LastRunStatus.SUCCESS) {
      lastRunEnd = LocalDateTime.now();
    }
  }

  @PostConstruct
  private void initDwhStatus() {
    cron = CronExpression.parse(dataProperties.getIncrementalSchedule());
    String prefix = dataProperties.getDwhRootPrefix();
    Preconditions.checkState(prefix != null && !prefix.isEmpty());
    File rootPrefix = new File(prefix);
    File parent = rootPrefix.getParentFile();
    if (parent == null) {
      // TODO make the logic here working on different file-systems including distributed ones.
      parent = new File(".");
    }
    String noDirFilePrefix = rootPrefix.getName();
    File[] files =
        parent.listFiles(
            file -> {
              String fileName = file.getName();
              return fileName.startsWith(noDirFilePrefix);
            });
    Preconditions.checkState(files != null, "Make sure DWH prefix is a valid path!");
    String lastDwh = "";
    for (File f : files) {
      String fileName = f.getName();
      if (!fileName.startsWith(noDirFilePrefix + DataProperties.TIMESTAMP_PREFIX)) {
        // This is not necessarily an error; the user may want to bootstrap from an already created
        // DWH outside the control-panel framework, e.g., by running the batch pipeline directly.
        logger.warn(
            "DWH directory {} does not start with {}",
            files,
            noDirFilePrefix + DataProperties.TIMESTAMP_PREFIX);
      }
      if (lastDwh.isEmpty() || lastDwh.compareTo(fileName) < 0) {
        logger.debug("Found a more recent DWH {}", fileName);
        lastDwh = fileName;
      }
    }
    if (lastDwh.isEmpty()) {
      logger.info("No DWH found; it should be created by running a full pipeline");
      currentDwh = null;
      lastRunEnd = null;
    } else {
      logger.info("Initializing with most recent DWH {}", lastDwh);
      currentDwh = DwhFiles.forRoot(new File(parent.getPath(), lastDwh).getPath());
      // There exists a DWH from before, so we set the scheduler to continue updating the DWH.
      lastRunEnd = LocalDateTime.now();
    }
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
  private void checkSchedule() throws IOException, PropertyVetoException {
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
      throws IOException, PropertyVetoException {
    DatabaseConfiguration dbConfig =
        DatabaseConfiguration.createConfigFromFile(options.getFhirDatabaseConfigPath());
    FhirContext fhirContext = FhirContext.forR4Cached();
    logger.info("Creating HAPI JDBC pipeline with options {}", options);
    return FhirEtl.buildHapiJdbcFetch(options, dbConfig, fhirContext);
  }

  synchronized void runBatchPipeline() throws IOException, PropertyVetoException {
    Preconditions.checkState(!isRunning(), "cannot start a pipeline while another one is running");
    FhirEtlOptions options = dataProperties.createBatchOptions();
    Pipeline pipeline = buildJdbcPipeline(options);
    if (pipeline == null) {
      logger.warn("No resources found to be fetched!");
      return;
    } else {
      currentPipeline = new PipelineThread(pipeline, this, dataProperties);
    }
    logger.info("Running full pipeline for DWH {}", options.getOutputParquetPath());
    // We will only have one thread for running pipelines hence no need for a thread pool.
    currentPipeline.start();
  }

  synchronized void runIncrementalPipeline() throws IOException, PropertyVetoException {
    // TODO do the same as above but read/set --since
    Preconditions.checkState(!isRunning(), "cannot start a pipeline while another one is running");
    Preconditions.checkState(
        currentDwh != null,
        "cannot start the incremental pipeline while there are no DWHs; run full pipeline");
    FhirEtlOptions options = dataProperties.createBatchOptions();
    String finalDwhRoot = options.getOutputParquetPath();
    // TODO move old incremental_run dir if there is one
    String incrementalDwhRoot = currentDwh.newIncrementalRunPath().toString();
    options.setOutputParquetPath(incrementalDwhRoot);
    String since = currentDwh.readTimestampFile().toString();
    options.setSince(since);
    Pipeline pipeline = buildJdbcPipeline(options);

    // The merger pipeline merges the original full DWH with the new incremental one.
    ParquetMergerOptions mergerOptions = PipelineOptionsFactory.as(ParquetMergerOptions.class);
    mergerOptions.setDwh1(currentDwh.getRoot());
    mergerOptions.setDwh2(incrementalDwhRoot);
    mergerOptions.setMergedDwh(finalDwhRoot);
    mergerOptions.setRunner(FlinkRunner.class);
    mergerOptions.setNumShards(dataProperties.getMaxWorkers());
    FlinkPipelineOptions flinkOptions = mergerOptions.as(FlinkPipelineOptions.class);
    flinkOptions.setMaxParallelism(dataProperties.getMaxWorkers());

    if (pipeline == null) {
      // TODO communicate this to the UI
      logger.info("No new resources to be fetched!");
      setLastRunStatus(LastRunStatus.SUCCESS);
    } else {
      // Creating a thread for running both pipelines, one after the other.
      currentPipeline = new PipelineThread(pipeline, mergerOptions, this, dataProperties);
      logger.info("Running incremental pipeline for DWH {} since {}", currentDwh.getRoot(), since);
      currentPipeline.start();
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

    PipelineThread(Pipeline pipeline, PipelineManager manager, DataProperties dataProperties) {
      Preconditions.checkArgument(pipeline.getOptions().as(FhirEtlOptions.class) != null);
      this.pipeline = pipeline;
      this.manager = manager;
      this.dataProperties = dataProperties;
      this.mergerOptions = null;
    }

    PipelineThread(
        Pipeline pipeline,
        ParquetMergerOptions mergerOptions,
        PipelineManager manager,
        DataProperties dataProperties) {
      Preconditions.checkArgument(pipeline.getOptions().as(FhirEtlOptions.class) != null);
      this.pipeline = pipeline;
      this.manager = manager;
      this.mergerOptions = mergerOptions;
      this.dataProperties = dataProperties;
    }

    @Override
    public void run() {
      try {
        FhirEtlOptions options = pipeline.getOptions().as(FhirEtlOptions.class);
        EtlUtils.runPipelineWithTimestamp(pipeline, options);
        if (dataProperties.isCreateHiveResourceTables()) {
          createHiveResources(
              options.getResourceList(),
              options.getTimestampSuffix(),
              options.getThriftServerParquetPath());
        }
        if (mergerOptions == null) { // Do not update DWH yet if this was an incremental run.
          manager.updateDwh(options.getOutputParquetPath());
        } else {
          FhirContext fhirContext = FhirContext.forR4Cached();
          Pipeline mergerPipeline = ParquetMerger.createMergerPipeline(mergerOptions, fhirContext);
          logger.info("Merger options are {}", mergerOptions);
          EtlUtils.runMergerPipelineWithTimestamp(mergerPipeline, mergerOptions);
          if (dataProperties.isCreateHiveResourceTables()) {
            createHiveResources(
                options.getResourceList(),
                options.getTimestampSuffix(),
                mergerOptions.getMergedDwh());
          }
          manager.updateDwh(mergerOptions.getMergedDwh());
        }
        manager.setLastRunStatus(LastRunStatus.SUCCESS);
      } catch (Exception e) {
        logger.error("exception while running pipeline: ", e);
        manager.setLastRunStatus(LastRunStatus.FAILURE);
      }
    }

    private void createHiveResources(
        String resourceList, String timestampSuffix, String thriftServerParquetPath)
        throws IOException, SQLException {

      logger.info("Establishing connection to Thrift server Hive");
      DatabaseConfiguration dbConfig =
          DatabaseConfiguration.createConfigFromFile(dataProperties.getThriftserverHiveConfig());

      logger.info("Creating resources on Thrift server Hive");
      ThriftServerHiveResourceManager thriftServerHiveResourceManager =
          new ThriftServerHiveResourceManager(
              dataProperties.getHiveJdbcDriver(),
              dbConfig.makeJdbsUrlFromConfig(),
              dbConfig.getDatabaseUser(),
              dbConfig.getDatabasePassword());
      thriftServerHiveResourceManager.createResources(
          resourceList, timestampSuffix, thriftServerParquetPath);
      logger.info("Created resources on Thrift server Hive");
    }
  }

  public enum LastRunStatus {
    NOT_RUN,
    SUCCESS,
    FAILURE
  }
}
