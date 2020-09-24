package org.acme.kafka;

import javax.enterprise.context.ApplicationScoped;

import io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Broadcast;
import org.jboss.logging.Logger;


/**
 * A bean consuming data from the "prices" Kafka topic and applying some conversion.
 * The result is pushed to the "my-data-stream" stream which is an in-memory stream.
 */
@SuppressWarnings("unchecked")
@ApplicationScoped
public class PriceConverter {

    private static final Logger LOGGER = Logger.getLogger("Price-Converter");

    private static final double CONVERSION_RATE = 0.88;

    // Consume from the `prices` channel and produce to the `my-data-stream` channel
    @Incoming("prices")
    @Outgoing("my-data-stream")
    // Send to all subscribers
    @Broadcast
    // Acknowledge the messages before calling this method.
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public Message<Double> process(Message<Integer> priceInUsd) {

        IncomingCloudEventMetadata<Integer> cloudEventMetadata = priceInUsd.getMetadata(IncomingCloudEventMetadata.class)
                .orElseThrow(() -> new IllegalArgumentException("Expected a Cloud Event"));

        LOGGER.infof("Received Cloud Events (spec-version: %s): source:  '%s', type: '%s', subject: '%s' ",
                cloudEventMetadata.getSpecVersion(),
                cloudEventMetadata.getSource(),
                cloudEventMetadata.getType(),
                cloudEventMetadata.getSubject().orElse("no subject"));

        return priceInUsd.withPayload(Integer.valueOf(priceInUsd.getPayload()) * CONVERSION_RATE);
    }

}
