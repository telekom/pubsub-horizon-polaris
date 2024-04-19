// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.task;

import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.helper.Republisher;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Republishes Events based on a  callbackUrl or a subscription id.
 * If a callbackUrl is passed it uses the subscription ids given in the callbackUrl2SubscriptionIdsCache.
 * If only a subscription id is passed, we can assume that the DeliveryType changed to SSE
 *
 * @author Tim Pütz
 * @since 3.0
 */
@Slf4j
public class RepublishingTask implements Runnable {
    protected final CircuitBreakerCacheService circuitBreakerCache;
    protected final HealthCheckCache healthCheckCache;
    protected final PartialSubscriptionCache partialSubscriptionCache;
    protected final KafkaTemplate<String, String> kafkaTemplate;
    protected final PolarisConfig polarisConfig;
    protected final HorizonTracer tracer;
    protected final MessageStateMongoRepo messageStateMongoRepo;
    protected Republisher republisher;
    private Slice<MessageStateMongoDocument> messageStatesToRepublish;


    public RepublishingTask(Slice<MessageStateMongoDocument> messageStatesToRepublish, ThreadPoolService threadPoolService) {
        this(threadPoolService);
        this.messageStatesToRepublish = messageStatesToRepublish;
    }


    protected RepublishingTask(ThreadPoolService threadPoolService) {
        this.circuitBreakerCache = threadPoolService.getCircuitBreakerCacheService();
        this.healthCheckCache = threadPoolService.getHealthCheckCache();
        this.messageStateMongoRepo = threadPoolService.getMessageStateMongoRepo();

        this.partialSubscriptionCache = threadPoolService.getPartialSubscriptionCache();
        this.kafkaTemplate = threadPoolService.getKafkaTemplate();
        this.polarisConfig = threadPoolService.getPolarisConfig();
        this.tracer = threadPoolService.getTracer();

        this.republisher = new Republisher(kafkaTemplate, partialSubscriptionCache, polarisConfig, tracer, threadPoolService.getEventWriter());
    }


    @Override
    public void run() {
        log.info("Start RepublishingTask for {} messageStatesToRepublish", messageStatesToRepublish.getNumberOfElements());
        republisher.pickAndRepublishBatch(messageStatesToRepublish);
        log.info("Finish RepublishingTask for {} messageStatesToRepublish", messageStatesToRepublish.getNumberOfElements());
    }

    protected void setCircuitBreakersToRepublishing(List<String> subscriptionIds) {
        // Set CircuitBreakerMessage status to REPUBLISHING
        log.info("Updating circuit breaker messages to REPUBLISHING for {} subscriptionIds", subscriptionIds.size());
        log.debug("subscriptionIds: {}", subscriptionIds);
        for (var subscriptionId : subscriptionIds) {
            setCircuitBreakerToRepublishing(subscriptionId);
        }
    }
    protected void setCircuitBreakerToRepublishing(String subscriptionId) {
        log.info("Updating circuit breaker messages to REPUBLISHING for subscriptionId: {}", subscriptionId);
        var oCBMessage = circuitBreakerCache.getCircuitBreakerMessage(subscriptionId);
        oCBMessage.ifPresent(circuitBreakerMessage -> circuitBreakerCache.updateCircuitBreakerStatus(circuitBreakerMessage.getSubscriptionId(), CircuitBreakerStatus.REPUBLISHING));
    }

    protected Slice<MessageStateMongoDocument> getMessageStatesFromDB(List<String> subscriptionIds, Date timestampLessThan, Pageable pageable) {
        // Get where delivery type == callback && status == WAITING, FAILED (CallbackUrlNotFoundException) (others get processed by scheduled delivering/failed task)
        log.info("Call findByStatusWaitingOrWithCallbackExceptionAndSubscriptionIdsAndTimestampLessThanEqual with subscriptionIds: {} and pageable: {}", subscriptionIds, pageable);
        return messageStateMongoRepo.findByStatusWaitingOrWithCallbackExceptionAndSubscriptionIdsAndTimestampLessThanEqual(List.of(Status.WAITING, Status.FAILED), subscriptionIds, timestampLessThan, pageable);

    }

    /**
     * Queries the DB, picks the consumer records and republishes them.
     *
     * @param subscriptionIds
     * @return The {@link MessageStateMongoDocument MessageStateMongoDocuments} whereof the EventMessage could not get picked from Kafka
     */
    protected void queryDbPickStatesAndRepublishMessages(List<String> subscriptionIds) {
        // We always stay at page 0, because the updated messages are not found by the query anymore (state messages gets set to PROCESSED or FAILED)
        Pageable pageable = PageRequest.of(0, polarisConfig.getRepublishingBatchSize(), Sort.by(Sort.Direction.ASC, "timestamp"));
        Slice<MessageStateMongoDocument> messageStateMongoDocuments;

        var timestamp = new Date();

        do {
            log.info("Loading max. {} event states from MongoDB", polarisConfig.getRepublishingBatchSize());
            messageStateMongoDocuments = getMessageStatesFromDB(subscriptionIds, timestamp, pageable);

            log.info("Found {} event states in MongoDb", messageStateMongoDocuments.getNumberOfElements());
            log.debug("messageStateMongoDocuments: {}", messageStateMongoDocuments);

            republisher.pickAndRepublishBatch(messageStateMongoDocuments);
        } while(messageStateMongoDocuments.hasNext());
    }
}
