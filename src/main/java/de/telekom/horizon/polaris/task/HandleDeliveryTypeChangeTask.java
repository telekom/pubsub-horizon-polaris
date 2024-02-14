package de.telekom.horizon.polaris.task;

import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;

import java.util.Date;
import java.util.List;

/**
 * Task for handling changes in delivery type and republishing messages accordingly.
 * Extends {@link RepublishingTask} to reuse common republishing functionality.
 */
@Slf4j
public class HandleDeliveryTypeChangeTask extends RepublishingTask {
    private final PartialSubscription newPartialSubscription;

    public HandleDeliveryTypeChangeTask(PartialSubscription newPartialSubscription, ThreadPoolService threadPoolService) {
        super(threadPoolService);

        this.newPartialSubscription = newPartialSubscription;
    }

    /**
     * Executes the task, including updating the circuit breaker status, querying and republishing messages, and closing the circuit breaker.
     */
    @Override
    public void run() {
        log.info("Start HandleDeliveryTypeChangeTask with newPartialSubscription: {}", newPartialSubscription);

        String subscriptionId = newPartialSubscription.subscriptionId();
        setCircuitBreakerToRepublishing(subscriptionId);

        queryDbPickStatesAndRepublishMessages(List.of(subscriptionId));

        log.info("Close circuit breaker, removing entries from circuit breaker cache for subscription id {}", subscriptionId);
        circuitBreakerCache.closeCircuitBreaker(subscriptionId);

        log.info("Finished HandleDeliveryTypeChangeTask with newPartialSubscription: {}", newPartialSubscription);
    }


    /**
     * Retrieves message states from the database based on the new delivery type and subscription IDs.
     * If the new delivery type is CALLBACK it returns all SSE subscription event messages in PROCESSED with that subscription IDs.
     *
     * @param subscriptionIds   The list of subscription IDs for which to retrieve message states.
     * @param timestampLessThan The timestamp indicating the maximum allowed timestamp for retrieved message states.
     * @param pageable          The pagination information for the query.
     * @return A slice of message states retrieved from the database.
     */
    @Override
    protected Slice<MessageStateMongoDocument> getMessageStatesFromDB(List<String> subscriptionIds, Date timestampLessThan, Pageable pageable) {
        // SSE -> Callback
        if(DeliveryType.CALLBACK.equals(newPartialSubscription.deliveryType())) {
            // Get where delivery type == sse && status = processed
            return messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdsAsc( List.of(Status.PROCESSED), DeliveryType.SERVER_SENT_EVENT, subscriptionIds, pageable );
        }

        return super.getMessageStatesFromDB(subscriptionIds, timestampLessThan, pageable);
    }
}
