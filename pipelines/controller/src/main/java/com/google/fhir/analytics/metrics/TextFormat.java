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
package com.google.fhir.analytics.metrics;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import java.io.IOException;
import java.io.Writer;
import java.util.Enumeration;

class TextFormat {

  // This class is not expected to be instantiated.
  private TextFormat() {}

  /** Content-type for Data Pipes text version 0.0.1. */
  public static final String CONTENT_TYPE_001 = "text/plain; version=0.0.1; charset=utf-8";

  /**
   * Write out the text version of the given MetricFamilySamples. Inspired from the TextFormat
   * implementation in <a href="https://github.com/prometheus/client_java">Prometheus Client</a>
   */
  public static void write(Writer writer, Enumeration<MetricFamilySamples> mfs) throws IOException {
    while (mfs.hasMoreElements()) {
      Collector.MetricFamilySamples metricFamilySamples = mfs.nextElement();
      for (Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples) {
        writer.write(sample.name);
        if (!sample.labelNames.isEmpty()) {
          writer.write('{');
          for (int i = 0; i < sample.labelNames.size(); ++i) {
            writer.write(sample.labelNames.get(i));
            writer.write("=\"");
            writeEscapedLabelValue(writer, sample.labelValues.get(i));
            writer.write("\",");
          }
          writer.write('}');
        }
        writer.write(' ');
        writer.write(Collector.doubleToGoString(sample.value));
        writer.write('\n');
      }
    }
  }

  private static void writeEscapedLabelValue(Writer writer, String s) throws IOException {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '\\':
          writer.append("\\\\");
          break;
        case '\"':
          writer.append("\\\"");
          break;
        case '\n':
          writer.append("\\n");
          break;
        default:
          writer.append(c);
      }
    }
  }
}
