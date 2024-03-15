// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.task;

import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.model.CallbackKey;
import de.telekom.horizon.polaris.service.SubscriptionRepublishingHolder;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;

import java.util.List;
import java.util.Optional;

/**
 * Task for handling successful health requests, updating health check data, and republishing messages.
 * Extends {@link RepublishingTask} to reuse common republishing functionality.
 */
@Slf4j
public class HandleSuccessfulHealthRequestTask extends RepublishingTask {
    private final String callbackUrl;
    private final HttpMethod httpMethod;

    private final SubscriptionRepublishingHolder subscriptionRepublishingHolder;

    protected HandleSuccessfulHealthRequestTask(ThreadPoolService threadPoolService) {
        super(threadPoolService);

        this.callbackUrl = "";
        this.httpMethod = HttpMethod.HEAD;
        this.subscriptionRepublishingHolder = threadPoolService.getSubscriptionRepublishingHolder();
    }

    public HandleSuccessfulHealthRequestTask(String callbackUrl, HttpMethod httpMethod, ThreadPoolService threadPoolService) {
        super(threadPoolService);

        this.callbackUrl = callbackUrl;
        this.httpMethod = httpMethod;
        this.subscriptionRepublishingHolder = threadPoolService.getSubscriptionRepublishingHolder();
    }

    /**
     * Executes the task, updating health check data, clearing subscription IDs, and republishing messages.
     */
    @Override
    public void run() {
        CallbackKey callbackKey = new CallbackKey(callbackUrl, httpMethod);

        if(subscriptionRepublishingHolder.isRepublishing(callbackKey)) {
            log.warn("Republishing already in progress for callbackUrl: {} and httpMethod: {}. skipping republishing to prevent multiple loops for single callback endpoint", callbackUrl, httpMethod);
            return;
        }
        this.subscriptionRepublishingHolder.startRepublishing(callbackKey);

        log.info("Start HandleSuccessfulHealthRequestTask for callbackUrl: {} and httpMethod: {}", callbackUrl, httpMethod);
        var oHealthCheckData = healthCheckCache.get(callbackUrl, httpMethod);
        log.debug("callbackUrl: {}", callbackUrl);
        log.debug("httpMethod: {}", httpMethod);
        if(oHealthCheckData.isEmpty()) {
            healthCheckCache.update(callbackUrl, httpMethod, false); // Is this necessary? If there is no entry, why do we want to update it?
            log.warn("Could not find a health check entry for the given callbackUrl & httpMethod. CallbackUrl: {}, HttpMethod: {}", callbackUrl, httpMethod);
            return;
        }

        log.debug("Removing subscriptionIds from healthCheckCache, incrementing the republish count & setting isThreadOpen to false");
        // We do this, because the DUDE could (re)open a (new) circuit breaker while we are republishing.
        // With this, the polaris that handles the opened circuit breaker can start a new health request task, but we only republishing the events for the current subscriptionIds
        // and also close (later) only the ones that we started with - the ones that were reopened
        var subscriptionIds = healthCheckCache.clearBeforeRepublishing(callbackUrl, httpMethod);
        log.debug("subscriptionIds: {}", subscriptionIds);

        republish(subscriptionIds);
        this.subscriptionRepublishingHolder.indicateRepublishingFinished(callbackKey);
        log.info("Finished HandleSuccessfulHealthRequestTask for callbackUrl: {} and httpMethod: {}", callbackUrl, httpMethod);
    }

    /**
     * Republishes messages for the specified subscription IDs and closing circuit breakers.
     *
     * @param subscriptionIds The list of subscription IDs for which to republish messages.
     */
    protected void republish(List<String> subscriptionIds) {
        setCircuitBreakersToRepublishing(subscriptionIds);
        queryDbPickStatesAndRepublishMessages(subscriptionIds);
        var stillRepublishingSubscriptionIds = subscriptionIds.stream()
                .map(circuitBreakerCache::getCircuitBreakerMessage)
                .filter(Optional::isPresent).map(Optional::get)
                .filter(circuitBreakerMessage -> CircuitBreakerStatus.REPUBLISHING.equals(circuitBreakerMessage.getStatus()))
                .map(CircuitBreakerMessage::getSubscriptionId)
                .toList();
        log.debug("Close circuit breakers for {}/{} stillRepublishingSubscriptionIds ids {} from subscriptionIds: {}", stillRepublishingSubscriptionIds.size(), subscriptionIds.size(), stillRepublishingSubscriptionIds, subscriptionIds);
        circuitBreakerCache.closeCircuitBreakersIfRepublishing(stillRepublishingSubscriptionIds); // If we close all, we can not reopen them later, because updateCircuitBreakerStatus needs an existing entry
    }
}
