// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.task;

import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerHealthCheck;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.model.HealthCheckData;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.service.WorkerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpMethod;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * <p>Compares current (new) subscription with old subscription.</p>
 * <p>Checks if:</p>
 * <ul>
 *     <li>Delivery type was changed from CALLBACK to SSE</li>
 *     <li>RequestMethod did change (HEAD/GET)</li>
 *     <li>CallbackUrl did change</li>
 * </ul>
 * <p>Can start the {@linkplain RepublishingTask} or the {@linkplain HealthRequestTask}.</p>
 * @see <a href="https://gitlab.devops.telekom.de/dhei/teams/pandora/notes/concepts/-/blob/main/horizon-statelesser/Horizon3_brainstorm_plunger.drawio" >FlowChart</a>
 * @author Tim Pütz
 */
@Slf4j
public class SubscriptionComparisonTask implements Runnable {
    private final PartialSubscription oldPartialSubscription;
    private final PartialSubscription currPartialSubscriptionOrNull; // Null if deleted
    private final HealthCheckCache healthCheckCache;
    private final CircuitBreakerCacheService circuitBreakerCache;
    private final PolarisConfig polarisConfig;
    private final ThreadPoolService threadPoolService;
    private final MessageStateMongoRepo messageStateMongoRepo;

    private final WorkerService workerService;

    public SubscriptionComparisonTask(PartialSubscription oldPartialSubscription, PartialSubscription currPartialSubscriptionOrNull, ThreadPoolService threadPoolService) {
        this.threadPoolService = threadPoolService;
        this.oldPartialSubscription = oldPartialSubscription;
        this.currPartialSubscriptionOrNull = currPartialSubscriptionOrNull;
        this.healthCheckCache = threadPoolService.getHealthCheckCache();
        this.circuitBreakerCache = threadPoolService.getCircuitBreakerCacheService();
        this.polarisConfig = threadPoolService.getPolarisConfig();
        this.messageStateMongoRepo = threadPoolService.getMessageStateMongoRepo();
        this.workerService = threadPoolService.getWorkerService();
    }

    /**
     * <p>Compares the current (new) subscription with the old subscription and initiates corresponding tasks based on changes.</p>
     * <p>Checks for the following conditions:</p>
     * <ul>
     *     <li>If the subscription was deleted.</li>
     *     <li>If another Polaris pod instance is already handling the same callbackUrl.</li>
     *     <li>If the delivery type has changed (from CALLBACK to SSE or vice versa).</li>
     *     <li>If there is no delivery type change (callback to callback) and handles circuit breaker conditions.</li>
     * </ul>
     */
    @Override
    public void run() {
        String subscriptionId = oldPartialSubscription.subscriptionId();
        String oldCallbackUrlOrNull = oldPartialSubscription.callbackUrl(); // can be null if old subscription was SSE

        // Handle deleted subscription
        if(currPartialSubscriptionOrNull == null) {
            cleanHealthCheckCacheFromSubscriptionId(oldPartialSubscription);
            log.info("No current partial subscription found, assuming it got deleted and returning.");
            return;
        }

        String currCallbackUrlOrNull = currPartialSubscriptionOrNull.callbackUrl(); // can be null of new subscription is SSE

        if (workerService.tryGlobalLock()) {
            try {
                if (workerService.tryClaim(subscriptionId)) {
                    if (hasDeliveryTypeChanged(DeliveryType.CALLBACK, DeliveryType.SERVER_SENT_EVENT)) {
                        cleanHealthCheckCacheFromSubscriptionId(oldPartialSubscription);
                        threadPoolService.startHandleDeliveryTypeChangeTask(currPartialSubscriptionOrNull);
                    } else if (hasDeliveryTypeChanged(DeliveryType.SERVER_SENT_EVENT, DeliveryType.CALLBACK)) {
                        // SSE -> Callback does not need to be handled extra, logic is same as for callback -> callback
                        // If polaris dies WHILE reproducing SSE -> Callback, those messages gets LOST, bc the information is nowhere persistent
                        threadPoolService.startHandleDeliveryTypeChangeTask(currPartialSubscriptionOrNull);

                    } else { // no delivery type change (callback -> callback)
                        if(currPartialSubscriptionOrNull.isCircuitBreakerOptOut()) {
                            String newCallbackUrlOrOldOrNull = Objects.requireNonNullElse(oldCallbackUrlOrNull, currCallbackUrlOrNull);
                            var currHttpMethod = currPartialSubscriptionOrNull.isGetMethodInsteadOfHead() ? HttpMethod.GET : HttpMethod.HEAD;

                            // Workaround to fix bug where WAITING events can't be republished for opt-out subscriptions:
                            healthCheckCache.add(newCallbackUrlOrOldOrNull, currHttpMethod, currPartialSubscriptionOrNull.subscriptionId());

                            // Check here if there are some events on status WAITING
                            int batchSize = polarisConfig.getPollingBatchSize();
                            Pageable pageable = PageRequest.of(0, batchSize, Sort.by(Sort.Direction.ASC, "timestamp"));

                            Slice<MessageStateMongoDocument> waitingEvents;
                            var foundWaitingEvents = false;

                            do {
                                waitingEvents = messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdAsc(List.of(Status.WAITING), DeliveryType.CALLBACK, subscriptionId, pageable);

                                if (!waitingEvents.isEmpty()) {
                                    foundWaitingEvents = true;

                                    log.warn("Waiting events: {}", waitingEvents.getNumberOfElements());
                                    log.warn("Found waiting events for subscription {} and start to handleSuccessfulHealthRequestTask", currPartialSubscriptionOrNull);

                                    threadPoolService.startHandleSuccessfulHealthRequestTask(newCallbackUrlOrOldOrNull, currHttpMethod);
                                } else {
                                    log.warn("No waiting events found for subscription {}", currPartialSubscriptionOrNull);
                                }

                                pageable = pageable.next();
                            } while (waitingEvents.hasNext());

                            if (!foundWaitingEvents) {
                                log.warn("No waiting events found for subscription {} and start HandleSuccessfulHealthRequestTask", currPartialSubscriptionOrNull);
                                threadPoolService.startHandleSuccessfulHealthRequestTask(newCallbackUrlOrOldOrNull, currHttpMethod);
                            }

                            return;
                        }

                        boolean isCallbackUrlSame = Objects.equals(oldCallbackUrlOrNull, currCallbackUrlOrNull);
                        if(isCallbackUrlSame) { // callbackUrl did not change -> start healthRequestTask
                            boolean didHttpMethodChange = !Objects.equals(oldPartialSubscription.isGetMethodInsteadOfHead(), currPartialSubscriptionOrNull.isGetMethodInsteadOfHead());
                            if (didHttpMethodChange) {
                                cleanHealthCheckCacheFromSubscriptionId(oldPartialSubscription);
                            }
                            startNewHealthRequestTask(currPartialSubscriptionOrNull);
                            return;
                        }

                        var oCBMessage = circuitBreakerCache.getCircuitBreakerMessage(subscriptionId);
                        if(oCBMessage.isEmpty()) { // no openCircuitBreakers for new callbackUrl -> no need to do something
                            log.info("No circuit breaker messages found for subscription {}", currPartialSubscriptionOrNull);
                        } else { // new callbackUrl with openCircuitBreaker
                            cleanHealthCheckCacheFromSubscriptionId(oldPartialSubscription);
                            var cbMessage = oCBMessage.get();
                            cbMessage.setCallbackUrl(currCallbackUrlOrNull);
                            circuitBreakerCache.updateCircuitBreakerMessage(cbMessage);
                            startNewHealthRequestTask(currPartialSubscriptionOrNull);
                        }
                    }
                }
            } finally {
                workerService.globalUnlock();
            }
        }
    }

    /**
     * Checks if the delivery type has changed.
     *
     * @param fromDeliveryType The delivery type to check for change.
     * @param toDeliveryType   The delivery type to check against.
     * @return True if the delivery type has changed, false otherwise.
     */
    private boolean hasDeliveryTypeChanged(DeliveryType fromDeliveryType, DeliveryType toDeliveryType) {
        DeliveryType oldDeliveryType = oldPartialSubscription.deliveryType();
        DeliveryType newDeliveryType = currPartialSubscriptionOrNull.deliveryType();

        boolean wasFromDeliveryType = fromDeliveryType.equals(oldDeliveryType);
        boolean isToDeliveryTypeNow = toDeliveryType.equals(newDeliveryType);

        return wasFromDeliveryType && isToDeliveryTypeNow;
    }

    /**
     * Removes the subscriptionId from the HealthCache entry (if existing).
     * If (afterward) the HealthCheck entry has no more subscriptionIds, closes the thread for callbackUrl + httpMethod
     *
     * @param partialSubscription
     */
    private void cleanHealthCheckCacheFromSubscriptionId(PartialSubscription partialSubscription) {
        var callbackUrl = partialSubscription.callbackUrl();
        var httpMethod = partialSubscription.isGetMethodInsteadOfHead() ? HttpMethod.GET : HttpMethod.HEAD;
        if(callbackUrl == null) { return; }

        healthCheckCache.remove(callbackUrl, httpMethod, partialSubscription.subscriptionId());
        var oHealthAndSubscriptionIds = healthCheckCache.get(callbackUrl, httpMethod);
        if(oHealthAndSubscriptionIds.isPresent()) {
            var healthAndSubscriptionIds = oHealthAndSubscriptionIds.get();
            if(healthAndSubscriptionIds.getSubscriptionIds().isEmpty()) {
                healthCheckCache.update(callbackUrl, httpMethod, false);
                threadPoolService.stopHealthRequestTask(callbackUrl, httpMethod);
            }
        }
    }

    /**
     * Starts a new {@link HealthRequestTask} based on the current {@link PartialSubscription}.
     * If the health request exists and no thread is open, it returns {@code true}.
     * The health request data needs to exist; otherwise, no subscription ID for the callback URL was added.
     *
     * @param partialSubscription The current partial subscription.
     */
    private void startNewHealthRequestTask(PartialSubscription partialSubscription) {
        var currHttpMethod = partialSubscription.isGetMethodInsteadOfHead() ? HttpMethod.GET : HttpMethod.HEAD;
        // true, if health request exists and no thread is open.
        // health request data needs to exist, else no subscription id for callback url was added, which means that no head request needs to be done
        boolean shouldStartHealthRequest = healthCheckCache.add(partialSubscription.callbackUrl(), currHttpMethod, partialSubscription.subscriptionId());
        if (shouldStartHealthRequest) {
            var oHealthCheck = healthCheckCache.get(partialSubscription.callbackUrl(), currHttpMethod);

            var republishCount = oHealthCheck.map(HealthCheckData::getRepublishCount).orElse(0);
            var oLastCheckDate = oHealthCheck.map(HealthCheckData::getLastHealthCheckOrNull).map(CircuitBreakerHealthCheck::getLastCheckedDate);

            Duration cooldown = HealthRequestTask.calculateCooldown(republishCount);

            // Reset cooldown and republish count if needed
            if (oLastCheckDate.isPresent()) {
                Duration dur = Duration.between(Instant.now(), oLastCheckDate.get().toInstant()).abs();
                if (dur.toMinutes() >= polarisConfig.getRequestCooldownResetMins()) {
                    healthCheckCache.resetRepublishCount(partialSubscription.callbackUrl(), currHttpMethod);
                    cooldown = Duration.ofNanos(0);
                }
            }

            // This is the only place where we initially start the health request task (with a delay)
            threadPoolService.startHealthRequestTask(partialSubscription.callbackUrl(), partialSubscription.publisherId(), partialSubscription.subscriberId(), partialSubscription.environment(), currHttpMethod, cooldown);
        }
    }
}
