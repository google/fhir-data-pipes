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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

/** Helper for runtime logger-level configuration. */
public final class LoggingConfigurator {

  private static final String LOGGER_CONTEXT_CLASS = "ch.qos.logback.classic.LoggerContext";
  private static final String LOGGER_CLASS = "ch.qos.logback.classic.Logger";
  private static final String LEVEL_CLASS = "ch.qos.logback.classic.Level";
  private static final String WARN_LEVEL = "WARN";

  private static final String[] QUIET_LOGGERS =
      new String[] {"com.google.fhir.analytics", "ca.uhn.fhir", "com.cerner.bunsen"};

  private static final AtomicBoolean QUIET_MODE_APPLIED = new AtomicBoolean(false);

  private LoggingConfigurator() {}

  /** Applies runtime quiet mode (WARN/ERROR) for known noisy logger categories. */
  public static void applyQuietMode(boolean quietMode) {
    if (!quietMode || !QUIET_MODE_APPLIED.compareAndSet(false, true)) {
      return;
    }
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!LOGGER_CONTEXT_CLASS.equals(loggerFactory.getClass().getName())) {
      return;
    }
    try {
      Class<?> loggerContextClass = Class.forName(LOGGER_CONTEXT_CLASS);
      Class<?> loggerClass = Class.forName(LOGGER_CLASS);
      Class<?> levelClass = Class.forName(LEVEL_CLASS);
      Method getLoggerMethod = loggerContextClass.getMethod("getLogger", String.class);
      Method setLevelMethod = loggerClass.getMethod("setLevel", levelClass);
      Field warnField = levelClass.getField(WARN_LEVEL);
      Object warnLevel = warnField.get(null);
      for (String loggerName : QUIET_LOGGERS) {
        Object logger = getLoggerMethod.invoke(loggerFactory, loggerName);
        setLevelMethod.invoke(logger, warnLevel);
      }
    } catch (ReflectiveOperationException ignored) {
      return;
    }
  }
}
