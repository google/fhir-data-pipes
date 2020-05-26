package org.openmrs;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.ict4h.atomfeed.client.domain.Event;
import org.ict4h.atomfeed.client.service.EventWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirEventWorker implements EventWorker {
  private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);
  private String feedBaseUrl;

  FhirEventWorker(String feedBaseUrl) {
    this.feedBaseUrl = feedBaseUrl;
  }

  @Override
  public void process(Event event) {
    log.info("In process for event: " + event);
    String content = event.getContent();
    if (content == null || "".equals(content)) {
      log.warn("No content in event: " + event);
      return;
    }
    Gson gson = new Gson();
    try {
      JsonObject jsonObj = JsonParser.parseString(content).getAsJsonObject();
      String fhirResource = jsonObj.get("fhir").getAsString();
      if (fhirResource == null || "".equals(fhirResource)) {
        log.info("Skipping non-FHIR event " + event);
        return;
      }
      log.info("FHIR resource is: " + fhirResource);
    } catch (JsonParseException e) {
      log.error(String.format("Error parsing content %s of event %s", content, event.toString()));
    }
  }

  @Override
  public void cleanUp(Event event) {
    log.info("In clean-up!");
  }


}
