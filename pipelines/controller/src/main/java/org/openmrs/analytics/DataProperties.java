/*
 * Copyright 2020-2022 Google LLC
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
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

  static final String TIMESTAMP_PREFIX = "_TIMESTAMP_";

  private static final String GET_PREFIX = "get";

  private static final Set<String> EXCLUDED_ARGS =
      ImmutableSet.of("jobName", "optionsId", "stableUniqueNames", "userAgent");

  // TODO check if there is a better way to avoid this list; currently this is the list of all
  //   default annotations we use on pipeline options.
  private static final Class[] DEFAULT_ANNOTATIONS = {
    Default.String.class, Default.Integer.class, Default.Boolean.class, Default.Long.class
  };

  private String fhirServerUrl;

  private String dbConfig;

  private String dwhRootPrefix;

  private String incrementalSchedule;

  private String resourceList;

  private int maxWorkers;

  @PostConstruct
  void validateProperties() {
    CronExpression.parse(incrementalSchedule);
  }

  FhirEtlOptions createBatchOptions() {
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    logger.info("Converting options for fhirServerUrl {}", fhirServerUrl);
    options.setFhirServerUrl(fhirServerUrl);
    options.setFhirDatabaseConfigPath(dbConfig);
    options.setResourceList(resourceList);
    // Note we prefer to use a human-readable name but we do not rely on the timestamp being in
    // the DWH name; the reason for replacing `:` is easier copy/paste from the UI to `bash`.
    options.setOutputParquetPath(
        dwhRootPrefix + TIMESTAMP_PREFIX + Instant.now().toString().replaceAll(":", "-"));
    options.setRunner(FlinkRunner.class);
    FlinkPipelineOptions flinkOptions = options.as(FlinkPipelineOptions.class);
    flinkOptions.setMaxParallelism(getMaxWorkers());
    return options;
  }

  List<ConfigFields> getConfigParams() {
    // TODO automate generation of this list.
    return ImmutableList.of(
        new ConfigFields("fhirdata.fhirServerUrl", fhirServerUrl, "", ""),
        new ConfigFields("fhirdata.dwhRootPrefix", dwhRootPrefix, "", ""),
        new ConfigFields("fhirdata.incrementalSchedule", incrementalSchedule, "", ""),
        new ConfigFields("fhirdata.resourceList", resourceList, "", ""),
        new ConfigFields("fhirdata.maxWorkers", String.valueOf(maxWorkers), "", ""),
        new ConfigFields("fhirdata.dbConfig", dbConfig, "", ""));
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
    public String name;
    public String value;
    public String description;
    public String def;
  }
}
