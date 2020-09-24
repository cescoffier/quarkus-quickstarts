package org.acme.kafka;

import java.time.Duration;
import java.util.Random;

import javax.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

/**
 * A bean producing random prices every 5 seconds.
 * The prices are written to a Kafka topic (prices). The Kafka configuration is specified in the application configuration.
 *
 * The prices are sent as binary Cloud Events. Cloud Event configuration is in the `application.properties` file.
 * It is also possible to configure the Cloud Event from the code itself. For this, check the SmallRye Reactive Messaging
 * documentation.
 */
@ApplicationScoped
public class PriceGenerator {

    private Random random = new Random();

    @Outgoing("generated-price")
    public Multi<Integer> generate() {
        return Multi.createFrom().ticks().every(Duration.ofSeconds(5))
                .onOverflow().drop()
                .onItem().transform(tick -> random.nextInt(100));
    }

}
