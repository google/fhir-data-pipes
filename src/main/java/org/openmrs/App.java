package org.openmrs;

import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Hello world!
 *
 */
public class App
{
    private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);

    public static void main( String[] args ) throws InterruptedException, URISyntaxException
    {
        if (args.length != 1) {
            System.out.println("You should pass exactly one URI argument!");
            return;
        }
        /* We can load ApplicationContext from the openmrs dependency like this but that
        fails with other exceptions (e.g., related to JdbcEnvironment when creating sessionFactory)
        and in general there should be an easier/more lightweight way of just using the
        AtomFeedClient which is all we need!
         */
        ClassPathXmlApplicationContext ctx =
            new ClassPathXmlApplicationContext("classpath:/applicationContext-service.xml");

        System.out.println( "Started listening on the feed " + args[0]);
        FeedConsumer feedConsumer = new FeedConsumer(args[0]);
        while (true) {
            log.info("Listen then wait for 1 second!");
            feedConsumer.listen();
            Thread.sleep(1000);
        }
    }
}
