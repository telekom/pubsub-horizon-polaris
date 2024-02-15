// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CouldNotDetermineWorkingSetException;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.PodService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.task.RepublishingTask;
import de.telekom.horizon.polaris.task.SubscriptionComparisonTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

/**
 * Manages circuit breakers, handling their states and assignment to the current pod.
 */
@Component
@Slf4j
public class CircuitBreakerManager {
    private final ThreadPoolService threadPoolService;
    private final CircuitBreakerCacheService circuitBreakerCacheService;
    private final PodService podService;
    private final PolarisConfig polarisConfig;
    private final PartialSubscriptionCache partialSubscriptionCache;


    public CircuitBreakerManager(ThreadPoolService threadPoolService) {
        this.threadPoolService = threadPoolService;
        this.circuitBreakerCacheService = threadPoolService.getCircuitBreakerCacheService();
        this.podService = threadPoolService.getPodService();
        this.partialSubscriptionCache = threadPoolService.getPartialSubscriptionCache();
        this.polarisConfig = threadPoolService.getPolarisConfig();
    }


    /**
     * Rebalances circuit breakers when a pod is removed. Does that by trying to reclaim all circuit breaker messages for itself.
     * But only gets the ones assigned with its callback url hash and where the removed pod was assigned.
     *
     * @param removedPod The pod that was removed.
     */
    public void rebalanceFromRemovedPod(String removedPod) {
        log.info("Start rebalanceFromRemovedPod for removed pod: {}", removedPod);
        int page = 0;
        int sumOfClaimedCircuitBreakerMessages = 0;
        List<CircuitBreakerMessage> openCBs;
        try {
            do {
                openCBs = circuitBreakerCacheService.getCircuitBreakerMessages(page++, polarisConfig.getPollingBatchSize());

                int nrOfClaimedMessages = claimCircuitBreakerMessagesIfPossible(openCBs);
                sumOfClaimedCircuitBreakerMessages += nrOfClaimedMessages;
                // if a message was claimed we stay on the same page, because the result of the query is now different for the same page
                // if we claim 10 messages on page 1 and the next 10 messages to claim will move to page 1, but we would go page 2
                // missing the 10 messages. Thats why we stay on page 1 as long as we claim messages
                if (nrOfClaimedMessages > 0) {
                    page--;
                }
            } while (openCBs.size() >= polarisConfig.getPollingBatchSize());
        } catch (CouldNotDetermineWorkingSetException e) {
            log.error(e.getMessage(), e);
        }
        log.info("Finished rebalanceFromRemovedPod for removed pod: {}, claimed {} circuit breaker messages", removedPod, sumOfClaimedCircuitBreakerMessages);
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
        try {
            List<CircuitBreakerMessage> openCBs;
            do {
                openCBs = circuitBreakerCacheService.getCircuitBreakerMessages(page++, polarisConfig.getPollingBatchSize(), status);
                log.info("{} circuit breakers: {}", status, openCBs);

                int nrOfClaimedMessages = claimCircuitBreakerMessagesIfPossible(openCBs);
                sumOfClaimedCircuitBreakerMessages += nrOfClaimedMessages;

                // see rebalanceFromRemovedPod
                if (nrOfClaimedMessages > 0) {
                    page--;
                }
            }
            while (openCBs.size() >= polarisConfig.getPollingBatchSize());
        } catch (CouldNotDetermineWorkingSetException e) {
            log.error(e.getMessage(), e);
        }
        log.info("Finished loadAndProcessCircuitBreakerMessages for circuit breaker status: {}, claimed {} circuit breaker messages", status, sumOfClaimedCircuitBreakerMessages);
    }

    /**
     * Claims circuit breaker messages if possible and triggers corresponding {@link SubscriptionComparisonTask} tasks.
     *
     * @param circuitBreakerMessages List of circuit breaker messages to process.
     * @return The number of claimed circuit breaker messages.
     * @throws CouldNotDetermineWorkingSetException If working set determination fails.
     */
    private int claimCircuitBreakerMessagesIfPossible(List<CircuitBreakerMessage> circuitBreakerMessages) throws CouldNotDetermineWorkingSetException {
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
     * @throws CouldNotDetermineWorkingSetException If working set determination fails.
     */
    public boolean claimCircuitBreakerMessageIfPossible(CircuitBreakerMessage circuitBreakerMessage) throws CouldNotDetermineWorkingSetException {
        CircuitBreakerStatus status = circuitBreakerMessage.getStatus();
        String subscriptionId = circuitBreakerMessage.getSubscriptionId();
        boolean wasCircuitBreakerMessageChanged = false;

        if (!podService.shouldCircuitBreakerMessageBeHandledByThisPod(circuitBreakerMessage)) {
            log.info("Claiming circuit breaker message for subscriptionId {} was not possible, because this pod should not handle this callbackUrl or it is already assigned to another pod.", subscriptionId);
            return false;
        }

        var oPartialSubscription = partialSubscriptionCache.get(subscriptionId);
        if (oPartialSubscription.isEmpty()) {
            log.warn("Could not find PartialSubscription with id {} for claiming circuit breaker message, closing", subscriptionId);
            circuitBreakerCacheService.closeCircuitBreaker(subscriptionId);
            return false;
        }

        String oldAssignedPodId = circuitBreakerMessage.getAssignedPodId();
        boolean willAssignmentChange = !Objects.equals(oldAssignedPodId, polarisConfig.getPodName());
        if (willAssignmentChange) {
            circuitBreakerMessage.setAssignedPodId(polarisConfig.getPodName());
            wasCircuitBreakerMessageChanged = true;
        }

        if (CircuitBreakerStatus.OPEN.equals(status)) {
            circuitBreakerMessage.setStatus(CircuitBreakerStatus.CHECKING);
            wasCircuitBreakerMessageChanged = true;
        }

        if (wasCircuitBreakerMessageChanged) {
            circuitBreakerCacheService.updateCircuitBreakerMessage(circuitBreakerMessage);
        }

        PartialSubscription partialSubscription = oPartialSubscription.get();
        PartialSubscription oldPartialSubscription = new PartialSubscription(circuitBreakerMessage.getEnvironment(), subscriptionId, partialSubscription.publisherId(), circuitBreakerMessage.getSubscriberId(), circuitBreakerMessage.getCallbackUrl(), DeliveryType.CALLBACK, partialSubscription.isGetMethodInsteadOfHead(), partialSubscription.isCircuitBreakerOptOut());

        threadPoolService.startSubscriptionComparisonTask(oldPartialSubscription, partialSubscription);

        return wasCircuitBreakerMessageChanged;
    }
}
