package org.openmrs;

import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * A standalone app that listens on Atom Feeds of an OpenMRS server and translates the
 * changes in OpenMRS to FHIR resources that are exported to GCP FHIR store.
 */
public class FhirStreaming
{
    private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);

    public static void main( String[] args ) throws InterruptedException, URISyntaxException
    {
        if (args.length != 6) {
            System.out.println("You should pass exactly six arguments:");
            System.out.println("1) url: the base url of the OpenMRS server (ending in 'openmrs').");
            System.out.println(
                "2) JSESSIONID: the value of this cookie after logging into the OpenMRS server.");
            System.out.println("3) gcp_project_id of the FHIR store.");
            System.out.println("4) gcp_location of the FHIR store: e.g., 'us-central1'.");
            System.out.println("5) dataset name of the FHIR store.");
            System.out.println("6) FHIR store name.");
            System.out.println("Note it is expected that a MySQL DB with name `atomfeed_client` \n"
                + "exists (configurable in `hibernate.default.properties`) with tables \n"
                + "'failed_events' and 'markers' to track feed progress. \n"
                + "Use utils/create_db.sql to create these. \n");
            return;
        }
        /* We can load ApplicationContext from the openmrs dependency like this but that
        fails with other exceptions (e.g., related to JdbcEnvironment when creating sessionFactory)
        and in general there should be an easier/more lightweight way of just using the
        AtomFeedClient which is all we need!
         */
        ClassPathXmlApplicationContext ctx =
            new ClassPathXmlApplicationContext("classpath:/applicationContext-service.xml");

        System.out.println("Started listening on the feed " + args[0]);
        FeedConsumer feedConsumer = new FeedConsumer(args[0], args[1], args[2], args[3], args[4],
            args[5]);
        while (true) {
            feedConsumer.listen();
            Thread.sleep(3000);
        }
    }
}
