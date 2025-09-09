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

import ca.uhn.fhir.context.FhirVersionEnum;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import javax.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

/**
 * This class contains all configuration parameters of the controller app. Pipeline option instances
 * are created from these parameters. As all Spring configurations, these can be configured through
 * a config file, command line arguments, Java properties, or environment variables.
 */
@ConfigurationProperties("fhirdata")
@Getter
@Setter
@Component
public class DataProperties {

  private static final Logger logger = LoggerFactory.getLogger(DataProperties.class.getName());

  private static final String GET_PREFIX = "get";

  private static final Set<String> EXCLUDED_ARGS =
      Set.of("jobName", "optionsId", "stableUniqueNames", "userAgent");

  // TODO check if there is a better way to avoid this list; currently this is the list of all
  //   default annotations we use on pipeline options.
  private static final Class[] DEFAULT_ANNOTATIONS = {
    Default.String.class, Default.Integer.class, Default.Boolean.class, Default.Long.class
  };

  @SuppressWarnings("NotInitialized")
  private FhirFetchMode fhirFetchMode;

  @SuppressWarnings("NotInitialized")
  private String fhirServerUrl;

  @SuppressWarnings("NotInitialized")
  private String dbConfig;

  @SuppressWarnings("NotInitialized")
  private String dwhRootPrefix;

  @SuppressWarnings("NotInitialized")
  private String incrementalSchedule;

  @SuppressWarnings("NotInitialized")
  private String purgeSchedule;

  @SuppressWarnings("NotInitialized")
  private int numOfDwhSnapshotsToRetain;

  @SuppressWarnings("NotInitialized")
  private String resourceList;

  @SuppressWarnings("NotInitialized")
  private int numThreads;

  @SuppressWarnings("NotInitialized")
  private String thriftserverHiveConfig;

  @SuppressWarnings("NotInitialized")
  private boolean createHiveResourceTables;

  @SuppressWarnings("NotInitialized")
  private String hiveResourceViewsDir;

  @SuppressWarnings("NotInitialized")
  private String viewDefinitionsDir;

  @SuppressWarnings("NotInitialized")
  private boolean createParquetViews;

  @SuppressWarnings("NotInitialized")
  private String sinkDbConfigPath;

  @SuppressWarnings("NotInitialized")
  private String fhirServerPassword;

  @SuppressWarnings("NotInitialized")
  private String fhirServerUserName;

  @SuppressWarnings("NotInitialized")
  private boolean autoGenerateFlinkConfiguration;

  @SuppressWarnings("NotInitialized")
  private String fhirServerOAuthTokenEndpoint;

  @SuppressWarnings("NotInitialized")
  private String fhirServerOAuthClientId;

  @SuppressWarnings("NotInitialized")
  private String fhirServerOAuthClientSecret;

  @SuppressWarnings("NotInitialized")
  private boolean checkPatientEndpoint;

  @SuppressWarnings("NotInitialized")
  private String sinkFhirServerUrl;

  @SuppressWarnings("NotInitialized")
  public String sinkUserName;

  @SuppressWarnings("NotInitialized")
  public String sinkPassword;

  @SuppressWarnings("NotInitialized")
  private String structureDefinitionsPath;

  @SuppressWarnings("NotInitialized")
  private int rowGroupSizeForParquetFiles;

  @SuppressWarnings("NotInitialized")
  private FhirVersionEnum fhirVersion;

  @SuppressWarnings("NotInitialized")
  private int recursiveDepth;

  @SuppressWarnings("NotInitialized")
  private boolean generateParquetFiles;

  @PostConstruct
  void validateProperties() {
    CronExpression.parse(incrementalSchedule);
    Preconditions.checkState(
        FhirFetchMode.FHIR_SEARCH.equals(fhirFetchMode)
            || FhirFetchMode.BULK_EXPORT.equals(fhirFetchMode)
            || FhirFetchMode.HAPI_JDBC.equals(fhirFetchMode)
            || FhirFetchMode.OPENMRS_JDBC.equals(fhirFetchMode),
        "fhirFetchMode should be one of FHIR_SEARCH || BULK_EXPORT || HAPI_JDBC || OPENMRS_JDBC for"
            + " the controller application");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(fhirServerUrl) || !Strings.isNullOrEmpty(dbConfig),
        "At least one of fhirServerUrl or dbConfig should be set!");

    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(dwhRootPrefix), "dwhRootPrefix is required!");
    Preconditions.checkState(fhirVersion != null, "FhirVersion cannot be empty");
    Preconditions.checkState(!createHiveResourceTables || !thriftserverHiveConfig.isEmpty());
    Preconditions.checkState(!createHiveResourceTables || generateParquetFiles);
    Preconditions.checkState(!createParquetViews || generateParquetFiles);
  }

  private PipelineConfig.PipelineConfigBuilder addFlinkOptions(FhirEtlOptions options) {
    PipelineConfig.PipelineConfigBuilder pipelineConfigBuilder = PipelineConfig.builder();
    options.setRunner(FlinkRunner.class);
    FlinkPipelineOptions flinkOptions = options.as(FlinkPipelineOptions.class);
    if (numThreads > 0) {
      flinkOptions.setParallelism(numThreads);
    }

    pipelineConfigBuilder.fhirEtlOptions(options);
    return pipelineConfigBuilder;
  }

  PipelineConfig createRecreateViewsOptions(String dwhRoot) {
    Preconditions.checkState(!Strings.isNullOrEmpty(viewDefinitionsDir));
    Preconditions.checkState(!Strings.isNullOrEmpty(sinkDbConfigPath));
    Preconditions.checkState(!Strings.isNullOrEmpty(dwhRoot));
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    logger.info(
        "Creating options for recreating views in {} into DB config {} from DWH {} ",
        viewDefinitionsDir,
        sinkDbConfigPath,
        dwhRoot);
    options.setFhirFetchMode(FhirFetchMode.PARQUET);
    options.setParquetInputDwhRoot(dwhRoot);
    if (createParquetViews) {
      options.setOutputParquetViewPath(DwhFiles.newViewsPath(dwhRoot).toString());
    }
    options.setViewDefinitionsDir(viewDefinitionsDir);
    options.setSinkDbConfigPath(sinkDbConfigPath);
    options.setRecreateSinkTables(true);
    options.setStructureDefinitionsPath(Strings.nullToEmpty(structureDefinitionsPath));
    options.setFhirVersion(fhirVersion);
    if (rowGroupSizeForParquetFiles > 0) {
      options.setRowGroupSizeForParquetFiles(rowGroupSizeForParquetFiles);
    }
    if (resourceList != null) {
      options.setResourceList(resourceList);
    }
    return addFlinkOptions(options).build();
  }

  PipelineConfig createBatchOptions() {
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setFhirFetchMode(fhirFetchMode);
    logger.info("Converting options for fhirServerUrl {} and dbConfig {}", fhirServerUrl, dbConfig);
    if (!Strings.isNullOrEmpty(dbConfig)) {
      // TODO add OpenMRS support too; it should be easy but we want to make it explicit, such that
      //  if accidentally both `dbConfig` and `fhirServerUrl` are set, OpenMRS is not assumed.
      options.setJdbcModeHapi(true);
      options.setFhirDatabaseConfigPath(dbConfig);
    } else {
      options.setFhirServerUrl(Strings.nullToEmpty(fhirServerUrl));
      options.setFhirServerPassword(Strings.nullToEmpty(fhirServerPassword));
      options.setFhirServerUserName(Strings.nullToEmpty(fhirServerUserName));
      options.setFhirServerOAuthTokenEndpoint(Strings.nullToEmpty(fhirServerOAuthTokenEndpoint));
      options.setFhirServerOAuthClientId(Strings.nullToEmpty(fhirServerOAuthClientId));
      options.setFhirServerOAuthClientSecret(Strings.nullToEmpty(fhirServerOAuthClientSecret));
      options.setCheckPatientEndpoint(checkPatientEndpoint);
    }
    if (resourceList != null) {
      options.setResourceList(resourceList);
    }
    options.setViewDefinitionsDir(Strings.nullToEmpty(viewDefinitionsDir));
    options.setSinkDbConfigPath(Strings.nullToEmpty(sinkDbConfigPath));
    options.setStructureDefinitionsPath(Strings.nullToEmpty(structureDefinitionsPath));
    options.setFhirVersion(fhirVersion);
    options.setRecursiveDepth(recursiveDepth);
    if (rowGroupSizeForParquetFiles > 0) {
      options.setRowGroupSizeForParquetFiles(rowGroupSizeForParquetFiles);
    }

    if (!Strings.isNullOrEmpty(sinkFhirServerUrl)) {
      options.setFhirSinkPath(sinkFhirServerUrl);
      options.setSinkUserName(Strings.nullToEmpty(sinkUserName));
      options.setSinkPassword(Strings.nullToEmpty(sinkPassword));
    }

    // Using underscore for suffix as hyphens are discouraged in hive table names.
    String timestampSuffix = DwhFiles.safeTimestampSuffix();
    String newDwhRoot = dwhRootPrefix + DwhFiles.TIMESTAMP_PREFIX + timestampSuffix;
    options.setOutputParquetPath(newDwhRoot);

    options.setGenerateParquetFiles(generateParquetFiles);

    if (createParquetViews) {
      options.setOutputParquetViewPath(DwhFiles.newViewsPath(newDwhRoot).toString());
    }

    PipelineConfig.PipelineConfigBuilder pipelineConfigBuilder = addFlinkOptions(options);

    // Get hold of thrift server parquet directory from dwhRootPrefix config.
    String thriftServerParquetPathPrefix =
        dwhRootPrefix.substring(dwhRootPrefix.lastIndexOf("/") + 1, dwhRootPrefix.length());
    pipelineConfigBuilder.thriftServerParquetPath(
        thriftServerParquetPathPrefix + DwhFiles.TIMESTAMP_PREFIX + timestampSuffix);
    pipelineConfigBuilder.timestampSuffix(timestampSuffix);

    return pipelineConfigBuilder.build();
  }

  List<ConfigFields> getConfigParams() {
    // TODO automate generation of this list.
    return List.of(
        new ConfigFields(
            "fhirdata.fhirFetchMode", fhirFetchMode != null ? fhirFetchMode.name() : "", "", ""),
        new ConfigFields("fhirdata.fhirServerUrl", fhirServerUrl, "", ""),
        new ConfigFields("fhirdata.dwhRootPrefix", dwhRootPrefix, "", ""),
        new ConfigFields(
            "fhirdata.generateParquetFiles", String.valueOf(generateParquetFiles), "", ""),
        new ConfigFields("fhirdata.incrementalSchedule", incrementalSchedule, "", ""),
        new ConfigFields("fhirdata.purgeSchedule", purgeSchedule, "", ""),
        new ConfigFields(
            "fhirdata.numOfDwhSnapshotsToRetain",
            String.valueOf(numOfDwhSnapshotsToRetain),
            "",
            ""),
        new ConfigFields("fhirdata.resourceList", resourceList, "", ""),
        new ConfigFields("fhirdata.numThreads", String.valueOf(numThreads), "", ""),
        new ConfigFields("fhirdata.dbConfig", dbConfig, "", ""),
        new ConfigFields("fhirdata.viewDefinitionsDir", viewDefinitionsDir, "", ""),
        new ConfigFields("fhirdata.sinkDbConfigPath", sinkDbConfigPath, "", ""),
        new ConfigFields("fhirdata.fhirSinkPath", sinkFhirServerUrl, "", ""),
        new ConfigFields("fhirdata.sinkUserName", sinkUserName, "", ""),
        new ConfigFields("fhirdata.sinkPassword", sinkPassword, "", ""),
        new ConfigFields("fhirdata.structureDefinitionsPath", structureDefinitionsPath, "", ""),
        new ConfigFields("fhirdata.fhirVersion", fhirVersion.name(), "", ""),
        new ConfigFields(
            "fhirdata.rowGroupSizeForParquetFiles",
            String.valueOf(rowGroupSizeForParquetFiles),
            "",
            ""),
        new ConfigFields("fhirdata.recursiveDepth", String.valueOf(recursiveDepth), "", ""),
        new ConfigFields(
            "fhirdata.createParquetViews", String.valueOf(createParquetViews), "", ""));
  }

  ConfigFields getConfigFields(FhirEtlOptions options, Method getMethod) {
    String mName = getMethod.getName();
    Preconditions.checkArgument(mName.startsWith(GET_PREFIX));
    String paramName =
        Character.toLowerCase(mName.charAt(GET_PREFIX.length()))
            + mName.substring(GET_PREFIX.length() + 1);
    try {
      Object val = getMethod.invoke(options);
      String paramValue = (val == null ? "null" : val.toString());
      Description desc = AnnotationUtils.findAnnotation(getMethod, Description.class);
      String paramDesc = (desc == null ? "null" : desc.value());
      String defaultVal = "null";
      for (Class c : DEFAULT_ANNOTATIONS) {
        Object def = AnnotationUtils.findAnnotation(getMethod, c);
        if (def != null) {
          defaultVal = c.getMethod("value").invoke(def).toString();
        }
      }
      return new ConfigFields(paramName, paramValue, paramDesc, defaultVal);
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      logger.error("Exception during calling method {}", mName, e);
    }
    return new ConfigFields("", "", "", "");
  }

  void sortConfigList(List<ConfigFields> configFields) {
    configFields.sort(Comparator.comparing(c -> c.name));
  }

  List<ConfigFields> getConfigFieldsList(FhirEtlOptions options) {
    List<ConfigFields> pipelineConfigs = new ArrayList<>();
    for (Method method : options.getClass().getDeclaredMethods()) {
      if (!method.getName().startsWith(GET_PREFIX)) {
        continue;
      }
      DataProperties.ConfigFields config = getConfigFields(options, method);
      if (!EXCLUDED_ARGS.contains(config.getName())) {
        pipelineConfigs.add(config);
      }
    }
    sortConfigList(pipelineConfigs);
    return pipelineConfigs;
  }

  @Getter
  static class ConfigFields {
    ConfigFields(String n, String v, String d, String def) {
      name = n;
      value = v;
      description = d;
      this.def = def;
    }

    // We need to make these public as they need to be accessed when creating the UI model.
    public final String name;
    public final String value;
    public final String description;
    public final String def;
  }
}
