package org.openmrs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.openmrs.module.atomfeed.client.AtomFeedClient;
import org.openmrs.module.atomfeed.client.AtomFeedClientFactory;

public class FeedConsumer {
  private String feedBaseUrl;
  private AtomFeedClient feedClient;
  private List<String> categories;

  FeedConsumer(String feedBaseUrl) throws URISyntaxException {
    this.feedBaseUrl = feedBaseUrl;
    feedClient = AtomFeedClientFactory.createClient(new FhirEventWorker());
    // TODO check if this can be set by configuring above factory call.
    URI feedUri = new URI(feedBaseUrl);
    feedClient.setUri(feedUri);
    categories = new ArrayList<>();
    categories.add("patient");
    categories.add("encounter");
    categories.add("observation");
    categories.add("visit");
  }

  public void listen() {
    feedClient.process();
  }

}
