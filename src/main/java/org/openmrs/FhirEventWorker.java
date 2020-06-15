package org.openmrs;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
//import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1.CloudHealthcare;
import com.google.api.services.healthcare.v1.CloudHealthcareScopes;
//import com.google.auth.http.HttpCredentialsAdapter;
//import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.BaseResource;
import org.ict4h.atomfeed.client.domain.Event;
import org.ict4h.atomfeed.client.service.EventWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirEventWorker<T extends BaseResource> implements EventWorker {
  private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);

  private static final String FHIR_NAME = "projects/%s/locations/%s/datasets/%s/fhirStores/%s";
  private static final GsonFactory JSON_FACTORY = new GsonFactory();
  private static final NetHttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  // GoogleNetHttpTransport.newTrustedTransport()

  private String feedBaseUrl;
  private String jSessionId;
  private String resourceType;
  private Class<T> resourceClass;
  private FhirContext fhirContext = FhirContext.forR4();

  FhirEventWorker(String feedBaseUrl, String jSessionId, String resourceType,
      Class<T> resourceClass) {
    this.feedBaseUrl = feedBaseUrl;
    this.jSessionId = jSessionId;
    this.resourceType = resourceType;
    this.resourceClass = resourceClass;
  }

  private String fetchFhirResource(String urlStr) {
    try {
      URL url = new URL(urlStr);
      // TODO switch to Apache HTTP client.
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

  private T parserFhirJson(String fhirJson) {
    IParser parser = fhirContext.newJsonParser();
    return parser.parseResource(resourceClass, fhirJson);
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
      // TODO fix the validation issues at the OpenMRS level and remove this HACK!
      String modifiedJson = fhirJson.replace("\"status\":\"unknown\"",
          "\"status\":\"unknown\",\"class\":{\"code\":\"EMER\"}");
      log.info("Modified FHIR resource: " + modifiedJson);
      // Creating this resource is not really needed for the current purpose as we can simply
      // send the JSON payload to GCP FHIR store. This is kept for demonstration purposes.
      T resource = parserFhirJson(modifiedJson);
      String resourceId = resource.getIdElement().getIdPart();
      log.info(String.format("Parsed FHIR resource ID is %s and IdBase is %s", resourceId,
          resource.getIdBase()));
      uploadToCloud(resourceId, modifiedJson);
    } catch (JsonParseException e) {
      log.error(
          String.format("Error parsing event %s with error %s", event.toString(), e.toString()));
    }
  }

  @Override
  public void cleanUp(Event event) {
    log.info("In clean-up!");
  }

  // This follows the examples at:
  // https://github.com/GoogleCloudPlatform/java-docs-samples/healthcare/tree/master/healthcare/v1
  private void uploadToCloud(String resourceId, String jsonResource) {
    try {
      String fhirStoreName = String.format(
          FHIR_NAME, "bashir-variant", "us-central1", "test-dataset", "openmrs-incremental");
      //Create createFhir = new Create();
      fhirResourceCreate(fhirStoreName, this.resourceType, resourceId, jsonResource);
      /*
      CloudHealthcare.Builder builder = new CloudHealthcare.Builder(HTTP_TRANSPORT, JSON_FACTORY, null);
      CloudHealthcare cloudHealthcare = builder.build();
      cloudHealthcare.projects().locations().datasets().fhirStores().create();
       */
      //CloudHealthcare cloudHealthcare = new CloudHealthcare();
    } catch (IOException e) {
      log.error(
          String.format("IOException while using Google APIS: %s", e.toString()));
    } catch (URISyntaxException e) {
      log.error(
          String.format("URI syntax exception while using Google APIS: %s", e.toString()));
    }
  }

  private static void fhirResourceCreate(String fhirStoreName, String resourceType,
      String resourceId, String jsonResource)
      throws IOException, URISyntaxException {

    // Initialize the client, which will be used to interact with the service.
    CloudHealthcare client = createClient();
    HttpClient httpClient = HttpClients.createDefault();
    String uri = String.format(
        "%sv1/%s/fhir/%s/%s", client.getRootUrl(), fhirStoreName, resourceType, resourceId);
    log.info(String.format("URL is: %s", uri));
    URIBuilder uriBuilder = new URIBuilder(uri);
    log.info(String.format("Full URL is: %s", uriBuilder.build()));
    //StringEntity requestEntity = new StringEntity(
    //    "{\"resourceType\": \"" + resourceType + "\", \"language\": \"en\"}");
    StringEntity requestEntity = new StringEntity(jsonResource);

    HttpUriRequest request = RequestBuilder
        //.post()
        .put()
        //.get()
        //.patch()
        .setUri(uriBuilder.build())
        .setEntity(requestEntity)
        .addHeader("Content-Type", "application/fhir+json")
        .addHeader("Accept-Charset", "utf-8")
        .addHeader("Accept", "application/fhir+json")
        .addHeader("Authorization", "Bearer " + getAccessToken())
        .build();

    // Execute the request and process the results.
    HttpResponse response = httpClient.execute(request);
    HttpEntity responseEntity = response.getEntity();
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED
        && response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      log.error(String.format(
          "Exception creating FHIR resource: %s", response.getStatusLine().toString()));
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      responseEntity.writeTo(byteStream);
      log.error(byteStream.toString());
      throw new RuntimeException();
    }
    System.out.print("FHIR resource created: ");
    responseEntity.writeTo(System.out);
  }

  private static CloudHealthcare createClient() throws IOException {
    // Use Application Default Credentials (ADC) to authenticate the requests
    // For more information see https://cloud.google.com/docs/authentication/production
    /*
    GoogleCredentials credentials =
        GoogleCredentials.getApplicationDefault()//;
            .createScoped(Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));
    HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
     */

    // Create a HttpRequestInitializer, which will provide a baseline configuration to all requests.
    // TODO resolve deprecation by using above GoogleCredentials pattern!
    final GoogleCredential credential =
        GoogleCredential.getApplicationDefault(HTTP_TRANSPORT, JSON_FACTORY)
            .createScoped(Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));
    HttpRequestInitializer requestInitializer = new HttpRequestInitializer() {
      @Override
      public void initialize(HttpRequest httpRequest) throws IOException {
        credential.initialize(httpRequest);
        httpRequest.setConnectTimeout(60000); // 1 minute connect timeout
        httpRequest.setReadTimeout(60000); // 1 minute read timeout
      }
    };

    // Build the client for interacting with the service.
    return new CloudHealthcare.Builder(HTTP_TRANSPORT, JSON_FACTORY, requestInitializer)
        .setApplicationName("your-application-name")
        .build();
  }

  private static String getAccessToken() throws IOException {
    // TODO refactor credential creation
    /*
    // TODO figure out why scope creation fails in this case.
    final GoogleCredentials credentials =
        GoogleCredentials.getApplicationDefault()//;
            .createScoped(Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));
    credentials.refreshAccessToken();
    return credentials.getAccessToken().getTokenValue();
     */
    final GoogleCredential credential =
        GoogleCredential.getApplicationDefault(HTTP_TRANSPORT, JSON_FACTORY)
            .createScoped(Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));
    credential.refreshToken();
    return credential.getAccessToken();
  }

}
