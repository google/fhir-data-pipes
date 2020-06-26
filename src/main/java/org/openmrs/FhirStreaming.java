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
        if (args.length != 2) {
            System.out.println("You should pass exactly two arguments:");
            System.out.println("1) url: the base url of the OpenMRS server (ending in 'openmrs').");
            System.out.println(
                "2) JSESSIONID: the value of this cookie after logging into the OpenMRS server.");
            System.out.println("Note it is expected that a MySQL DB with name `atomfeed_client` \n"
                + "exists (configurable in `hibernate.default.properties`) with tables \n"
                + "'failed_events' and 'markers' to track feed progress. \n"
                + "Use scripts/create_db.sql to create these. \n");
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
        FeedConsumer feedConsumer = new FeedConsumer(args[0], args[1]);
        while (true) {
            feedConsumer.listen();
            Thread.sleep(3000);
        }
    }
}
