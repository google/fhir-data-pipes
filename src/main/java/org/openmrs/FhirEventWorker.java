package org.openmrs;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import org.hl7.fhir.r4.model.Encounter;
import org.ict4h.atomfeed.client.domain.Event;
import org.ict4h.atomfeed.client.service.EventWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirEventWorker/*<T>*/ implements EventWorker {
  private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);
  private String feedBaseUrl;
  private String jSessionId;
  private String resourceName;
  private FhirContext fhirContext = FhirContext.forR4();

  FhirEventWorker(String feedBaseUrl, String jSessionId, String resourceName) {
    this.feedBaseUrl = feedBaseUrl;
    this.jSessionId = jSessionId;
    this.resourceName = resourceName;
  }

  private String fetchFhirResource(String urlStr) {
    try {
      URL url = new URL(urlStr);
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");
      con.addRequestProperty("Content-Type", "application/json");
      con.setRequestProperty("Cookie", "JSESSIONID=" + this.jSessionId);
      int status = con.getResponseCode();
      if (status != 200) {
        log.error("Error " + status + " reading from url " + urlStr);
        return "";
      }
      BufferedReader inputBuffer = new BufferedReader(
          new InputStreamReader(con.getInputStream()));
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

  private Encounter/*T */parserFhirJson(String fhirJson) {
    Gson gson = new Gson();
    /*
    Type genericType = new TypeToken<T>() {}.getType();
    return gson.fromJson(fhirJson, genericType);
    //return gson.fromJson(fhirJson, Encounter.class);
     */
    IParser parser = fhirContext.newJsonParser();
    return parser.parseResource(Encounter.class, fhirJson);
  }

  @Override
  public void process(Event event) {
    log.info("In process for event: " + event);
    String content = event.getContent();
    if (content == null || "".equals(content)) {
      log.warn("No content in event: " + event);
      return;
    }
    try {
      JsonObject jsonObj = JsonParser.parseString(content).getAsJsonObject();
      String fhirUrl = jsonObj.get("fhir").getAsString();
      if (fhirUrl == null || "".equals(fhirUrl)) {
        log.info("Skipping non-FHIR event " + event);
        return;
      }
      log.info("FHIR resource URL is: " + fhirUrl);
      String fhirJson = fetchFhirResource(feedBaseUrl + fhirUrl);
      log.info("Fetched FHIR resource: " + fhirJson);
      /*T*/Encounter resource = parserFhirJson(fhirJson);
      log.info("Parsed FHIR resource is: " + resource);
    } catch (JsonParseException e) {
      log.error(
          String.format("Error parsing event %s with error %s", event.toString(), e.toString()));
    }
  }

  @Override
  public void cleanUp(Event event) {
    log.info("In clean-up!");
  }


}
