package org.openmrs;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import org.ict4h.atomfeed.client.domain.Event;
import org.ict4h.atomfeed.client.service.EventWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirEventWorker implements EventWorker {
  private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);
  private String feedBaseUrl;
  private String jSessionId;

  FhirEventWorker(String feedBaseUrl, String jSessionId) {
    this.feedBaseUrl = feedBaseUrl;
    this.jSessionId = jSessionId;
  }

  private String fetchFhirResource(String urlStr) {
    try {
      URL url = new URL(urlStr);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.addRequestProperty("Content-Type", "application/json");
      conn.setRequestProperty("Cookie", "JSESSIONID=" + this.jSessionId);
      int status = conn.getResponseCode();
      if (status != 200) {
        log.error("Error " + status + " reading from url " + urlStr);
        return "";
      }
      BufferedReader inputBuffer = new BufferedReader(
          new InputStreamReader(conn.getInputStream()));
      String inputLine;
      StringBuffer content = new StringBuffer();
      while ((inputLine = inputBuffer.readLine()) != null) {
        content.append(inputLine);
      }
      inputBuffer.close();
      return content.toString();
    } catch (MalformedURLException e) {
      log.error("Malformed FHIR url: " + urlStr + " exception: " + e);
      return "";
    } catch (IOException e) {
      log.error("Error in opening url: " + urlStr + " exception: " + e);
      return "";
    }
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
      String fhirUrl = jsonObj.get("fhir").getAsString();
      if (fhirUrl == null || "".equals(fhirUrl)) {
        log.info("Skipping non-FHIR event " + event);
        return;
      }
      log.info("FHIR resource URL is: " + fhirUrl);
      log.info("Fetched FHIR resource: " + fetchFhirResource(feedBaseUrl + fhirUrl));
    } catch (JsonParseException e) {
      log.error(String.format("Error parsing content %s of event %s", content, event.toString()));
    }
  }

  @Override
  public void cleanUp(Event event) {
    log.info("In clean-up!");
  }


}
