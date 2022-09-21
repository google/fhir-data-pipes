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

import org.apache.camel.main.Main;

// A streaming engine that captures incremental updates an OpenMRS mysql db server and translates
// the
// changes in OpenMRS to FHIR resources that are exported to GCP FHIR store.
public class Runner {

  private static final Main MAIN = new Main();

  // Main method that starts the streaming pipeline.
  public static void main(String[] args) throws Exception {
    ParquetUtil.initializeAvroConverters();

    MAIN.addRoutesBuilder(new DebeziumListener(args));
    // and enable graceful hangup support when container/process is shutdown
    MAIN.enableHangupSupport();
    // echo to console how to terminate
    System.out.println("\n\nThe pipeline in now running. You can press ctrl + c to stop.\n\n");
    MAIN.run();
  }
}
