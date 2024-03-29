// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.task;

import de.telekom.horizon.polaris.model.CallbackKey;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.SubscriptionRepublishingHolder;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;

import java.util.List;

/**
 * Task to republish events for a list of partial subscriptions, updating health check data and closing circuit breakers.
 * Extends {@link HandleSuccessfulHealthRequestTask} and is designed to be executed in a thread pool.
 *
 * @author Tim Pütz
 * @since 3.0
 */
@Slf4j
public class RepublishPartialSubscriptionsTask extends HandleSuccessfulHealthRequestTask {
    private final List<PartialSubscription> partialSubscriptions;

    private final SubscriptionRepublishingHolder subscriptionRepublishingHolder;

    public RepublishPartialSubscriptionsTask(List<PartialSubscription> partialSubscriptions, ThreadPoolService threadPoolService) {
        super(threadPoolService);

        this.subscriptionRepublishingHolder = threadPoolService.getSubscriptionRepublishingHolder();
        this.partialSubscriptions = partialSubscriptions;
    }

    /**
     * Executes the task to republish events for each partial subscription in the list.
     * Updates health check data and closes circuit breaker accordingly.
     */
    @Override
    public void run() {
        log.info("Start RepublishSubscriptionIdsTask for partialSubscriptions: {}", partialSubscriptions);

        for (var partialSubscription: partialSubscriptions) {
            String callbackUrl = partialSubscription.callbackUrl();
            var httpMethod = partialSubscription.isGetMethodInsteadOfHead() ? HttpMethod.GET : HttpMethod.HEAD;

            CallbackKey callbackKey = new CallbackKey(callbackUrl, httpMethod);
            if(subscriptionRepublishingHolder.isRepublishing(callbackKey)) {
                log.warn("Republishing already in progress for callbackUrl: {} and httpMethod: {}. skipping republishing to prevent multiple loops for single callback endpoint", callbackUrl, httpMethod);
                return;
            }
            this.subscriptionRepublishingHolder.startRepublishing(callbackKey);

            var oHealthCheckData = healthCheckCache.get(callbackUrl, httpMethod);
            if(oHealthCheckData.isEmpty()) {
                log.warn("Could not find a health check entry for the given callbackUrl & httpMethod. CallbackUrl: {}, HttpMethod: {}", callbackUrl, httpMethod);
                return;
            }

            log.debug("Removing subscriptionIds from healthCheckCache, incrementing the republish count & setting isThreadOpen to false");
            // We remove the subscription ids here, and after the task is done, we stop the health request task if needed (handleRepublishingCallbackFinished)
            var subscriptionIds = healthCheckCache.clearBeforeRepublishing(callbackUrl, httpMethod, List.of(partialSubscription.subscriptionId()));

            log.debug("subscriptionIds: {}", subscriptionIds);

            republish(subscriptionIds);
            this.subscriptionRepublishingHolder.indicateRepublishingFinished(callbackKey);
        }
        log.info("Finished RepublishSubscriptionIdsTask for partialSubscriptions: {}", partialSubscriptions);
    }
}
