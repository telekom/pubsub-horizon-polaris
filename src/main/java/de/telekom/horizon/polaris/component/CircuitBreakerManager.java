// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

import com.hazelcast.cluster.MembershipEvent;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CouldNotDetermineWorkingSetException;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.service.WorkerService;
import de.telekom.horizon.polaris.task.SubscriptionComparisonTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Manages circuit breakers, handling their states and assignment to the current pod.
 */
@Component
@Slf4j
public class CircuitBreakerManager {
    private final ThreadPoolService threadPoolService;
    private final CircuitBreakerCacheService circuitBreakerCacheService;
    private final PolarisConfig polarisConfig;
    private final PartialSubscriptionCache partialSubscriptionCache;
    private final WorkerService workerService;

    public CircuitBreakerManager(ThreadPoolService threadPoolService) {
        this.threadPoolService = threadPoolService;
        this.circuitBreakerCacheService = threadPoolService.getCircuitBreakerCacheService();
        this.partialSubscriptionCache = threadPoolService.getPartialSubscriptionCache();
        this.polarisConfig = threadPoolService.getPolarisConfig();
        this.workerService = threadPoolService.getWorkerService();
    }

    /**
     * Loads and processes circuit breaker messages based on their status.
     * Does that by trying to reclaim all circuit breaker messages for itself.
     *
     * @param status The status of circuit breakers to process.
     */
    public void loadAndProcessCircuitBreakerMessages(CircuitBreakerStatus status) {
        log.info("Start loadAndProcessCircuitBreakerMessages for circuit breaker status: {}", status);
        int sumOfClaimedCircuitBreakerMessages = 0;
        int page = 0;

        List<CircuitBreakerMessage> openCBs;
        do {
            openCBs = circuitBreakerCacheService.getCircuitBreakerMessages(page++, polarisConfig.getPollingBatchSize(), status);
            log.info("{} circuit breakers: {}", status, openCBs);

            int nrOfClaimedMessages = claimCircuitBreakerMessagesIfPossible(openCBs);
            sumOfClaimedCircuitBreakerMessages += nrOfClaimedMessages;

            if (nrOfClaimedMessages > 0) {
                page--;
            }
        }
        while (openCBs.size() >= polarisConfig.getPollingBatchSize());

        log.info("Finished loadAndProcessCircuitBreakerMessages for circuit breaker status: {}, claimed {} circuit breaker messages", status, sumOfClaimedCircuitBreakerMessages);
    }

    /**
     * Claims circuit breaker messages if possible and triggers corresponding {@link SubscriptionComparisonTask} tasks.
     *
     * @param circuitBreakerMessages List of circuit breaker messages to process.
     * @return The number of claimed circuit breaker messages.
     * @throws CouldNotDetermineWorkingSetException If working set determination fails.
     */
    private int claimCircuitBreakerMessagesIfPossible(List<CircuitBreakerMessage> circuitBreakerMessages) {
        int nrOfClaimedCircuitBreakerMessages = 0;
        for (var circuitBreakerMessage : circuitBreakerMessages) {
            boolean wasClaimed = claimCircuitBreakerMessageIfPossible(circuitBreakerMessage);
            if (wasClaimed) {
                nrOfClaimedCircuitBreakerMessages++;
            }
        }
        return nrOfClaimedCircuitBreakerMessages;
    }

    /**
     * Tries to claim a circuit breaker message by assigning its pod name.
     * After a successful claim, it starts a {@link SubscriptionComparisonTask} (OPEN/CHECKING)
     *
     * @param circuitBreakerMessage The circuit breaker message to process.
     * @return True if the circuit breaker message was claimed, false otherwise.
     */
    public boolean claimCircuitBreakerMessageIfPossible(CircuitBreakerMessage circuitBreakerMessage) {
        CircuitBreakerStatus status = circuitBreakerMessage.getStatus();
        String subscriptionId = circuitBreakerMessage.getSubscriptionId();

        boolean hasBeenClaimed = false;

        if (workerService.tryGlobalLock()) {
            try {
                if (workerService.tryClaim(subscriptionId)) {
                    var oPartialSubscription = partialSubscriptionCache.get(subscriptionId);
                    if (oPartialSubscription.isEmpty()) {
                        log.warn("Could not find PartialSubscription with id {} for claiming circuit breaker message, closing", subscriptionId);
                        circuitBreakerCacheService.closeCircuitBreaker(subscriptionId);
                        return false;
                    }

                    if (CircuitBreakerStatus.OPEN.equals(status)) {
                        circuitBreakerMessage.setStatus(CircuitBreakerStatus.CHECKING);
                        circuitBreakerCacheService.updateCircuitBreakerMessage(circuitBreakerMessage);
                        hasBeenClaimed = true;
                    }

                    PartialSubscription partialSubscription = oPartialSubscription.get();
                    PartialSubscription oldPartialSubscription = new PartialSubscription(circuitBreakerMessage.getEnvironment(), subscriptionId, partialSubscription.publisherId(), circuitBreakerMessage.getSubscriberId(), circuitBreakerMessage.getCallbackUrl(), DeliveryType.CALLBACK, partialSubscription.isGetMethodInsteadOfHead(), partialSubscription.isCircuitBreakerOptOut());

                    threadPoolService.startSubscriptionComparisonTask(oldPartialSubscription, partialSubscription);
                } else {
                    log.info("Claiming circuit breaker message for subscriptionId {} was not possible, because this pod should not handle this callbackUrl or it is already assigned to another pod.", subscriptionId);
                }
            } finally {
                workerService.globalUnlock();
            }
        }

        return hasBeenClaimed;
    }

    @EventListener
    public void reclaimCircuitBreaker(MembershipEvent membershipEvent) {
        if (membershipEvent.getEventType() == MembershipEvent.MEMBER_REMOVED) {
            int page = 0;

            List<CircuitBreakerMessage> openCBs;
            do {
                openCBs = circuitBreakerCacheService.getCircuitBreakerMessages(page++, polarisConfig.getPollingBatchSize());

                int nrOfClaimedMessages = claimCircuitBreakerMessagesIfPossible(openCBs);
                // if a message was claimed we stay on the same page, because the result of the query is now different for the same page
                // if we claim 10 messages on page 1 and the next 10 messages to claim will move to page 1, but we would go page 2
                // missing the 10 messages. Thats why we stay on page 1 as long as we claim messages
                if (nrOfClaimedMessages > 0) {
                    page--;
                }
            } while (openCBs.size() >= polarisConfig.getPollingBatchSize());
        }
    }
}
