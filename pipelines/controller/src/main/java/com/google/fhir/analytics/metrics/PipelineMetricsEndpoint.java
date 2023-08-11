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

import com.google.fhir.analytics.MetricsConstants;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Enumeration;
import org.springframework.boot.actuate.endpoint.Producible;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * This is a custom implementation of the spring actuator {@link Endpoint @Endpoint} which exports
 * only the pipeline metrics in a format that can be scraped by the Prometheus server.
 */
@WebEndpoint(id = "pipeline-metrics")
@Component
public class PipelineMetricsEndpoint {

  private static final int METRICS_SCRAPE_CHARS_EXTRA = 1024;

  private final CollectorRegistry collectorRegistry;

  private volatile int nextMetricsScrapeSize = 16;

  public PipelineMetricsEndpoint(CollectorRegistry collectorRegistry) {
    this.collectorRegistry = collectorRegistry;
  }

  @ReadOperation(producesFrom = TextOutputFormat.class)
  public WebEndpointResponse<String> scrape(TextOutputFormat format) {
    try {
      Writer writer = new StringWriter(this.nextMetricsScrapeSize);
      Enumeration<MetricFamilySamples> samples =
          this.collectorRegistry.filteredMetricFamilySamples(
              sample -> sample.startsWith(MetricsConstants.METRICS_NAMESPACE));
      format.write(writer, samples);

      String scrapePage = writer.toString();
      this.nextMetricsScrapeSize = scrapePage.length() + METRICS_SCRAPE_CHARS_EXTRA;

      return new WebEndpointResponse<>(scrapePage, format);
    } catch (IOException ex) {
      // This actually never happens since StringWriter doesn't throw an IOException
      throw new IllegalStateException("Writing metrics failed", ex);
    }
  }

  public enum TextOutputFormat implements Producible<TextOutputFormat> {
    CONTENT_TYPE_001(TextFormat.CONTENT_TYPE_001) {
      @Override
      void write(Writer writer, Enumeration<MetricFamilySamples> samples) throws IOException {
        TextFormat.write(writer, samples);
      }

      @Override
      public boolean isDefault() {
        return true;
      }
    };

    private final MimeType mimeType;

    TextOutputFormat(String mimeType) {
      this.mimeType = MimeTypeUtils.parseMimeType(mimeType);
    }

    @Override
    public MimeType getProducedMimeType() {
      return this.mimeType;
    }

    abstract void write(Writer writer, Enumeration<MetricFamilySamples> samples) throws IOException;
  }
}
