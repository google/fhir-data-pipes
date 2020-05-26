package org.openmrs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.ict4h.atomfeed.client.service.FeedClient;
import org.openmrs.module.atomfeed.client.AtomFeedClient;
import org.openmrs.module.atomfeed.client.AtomFeedClientFactory;

public class FeedConsumer {
  private List<AtomFeedClient> feedClients = new ArrayList<>();

  FeedConsumer(String feedBaseUrl, String jSessionId) throws URISyntaxException {
    List<String> categories = new ArrayList<>();
    categories.add("encounter");
    /* TODO add these and anything else needed, after debugging
    categories.add("patient");
    categories.add("visit");  // The FHIR resource ID in this case does not map to Encounter/!
    categories.add("observation");
     */
    for (String c : categories) {
      AtomFeedClient feedClient = AtomFeedClientFactory
          .createClient(new FhirEventWorker(feedBaseUrl, jSessionId));
      // TODO check if this can be set by configuring above factory call & finalize the feed number.
      URI feedUri = new URI(feedBaseUrl + "/ws/atomfeed/" + c + "/1");
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
