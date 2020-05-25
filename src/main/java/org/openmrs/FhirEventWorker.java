package org.openmrs;

import org.ict4h.atomfeed.client.domain.Event;
import org.ict4h.atomfeed.client.service.EventWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirEventWorker implements EventWorker {
  private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);

  @Override
  public void process(Event event) {
    log.info("In process for event: " + event);
  }

  @Override
  public void cleanUp(Event event) {
    log.info("In clean-up!");
  }


}
