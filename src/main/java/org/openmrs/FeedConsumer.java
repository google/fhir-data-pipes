package org.openmrs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.openmrs.module.atomfeed.client.AtomFeedClient;
import org.openmrs.module.atomfeed.client.AtomFeedClientFactory;

public class FeedConsumer {
  private List<AtomFeedClient> feedClients = new ArrayList<>();

  FeedConsumer(String feedBaseUrl, String jSessionId) throws URISyntaxException {
    // TODO what we really need is a list of pairs!
    Map<String, Class> categories = new LinkedHashMap<>();
    // TODO categories.put("patient", Patient.class);
    // TODO: Check why the FHIR resource ID in this case does not map to Encounter!
    // categories.put("visit", Encounter.class);
    categories.put("encounter", Encounter.class);
    categories.put("observation", Observation.class);
    // TODO add other FHIR resources that are implemented in OpenMRS.
    for (Map.Entry<String, Class> entry : categories.entrySet()) {
      AtomFeedClient feedClient = AtomFeedClientFactory.createClient(
          new FhirEventWorker(feedBaseUrl, jSessionId, "encounter", entry.getValue()));
      // TODO check if this can be set by configuring above factory call & finalize the feed number.
      URI feedUri = new URI(feedBaseUrl + "/ws/atomfeed/" + entry.getKey() + "/1");
      feedClient.setUri(feedUri);
      feedClients.add(feedClient);
    }
  }

  public void listen() {
    for (AtomFeedClient client : feedClients) {
      client.process();
    }
  }

}
