// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.kubernetes;

import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 * Handles events related to changes in {@link SubscriptionResource} in Kubernetes.
 * <p>
 * This class implements the {@link ResourceEventHandler} interface for Kubernetes Subscription resources.
 * It manages the cache of partial subscriptions and triggers tasks based on resource events such as addition,
 * update, and deletion of {@link SubscriptionResource}s.
 * </p>
 *
 * @see ResourceEventHandler
 * @see PartialSubscriptionCache
 */
@Service
@Slf4j
public class SubscriptionResourceEventHandler implements ResourceEventHandler<SubscriptionResource>  {
    private final PartialSubscriptionCache partialSubscriptionCache;
    private final ThreadPoolService threadPoolService;

    public SubscriptionResourceEventHandler(ThreadPoolService threadPoolService) {
        this.threadPoolService = threadPoolService;
        this.partialSubscriptionCache = threadPoolService.getPartialSubscriptionCache();
    }

    /**
     * Handles the addition of a {@link SubscriptionResource}.
     *
     * @param resource The added {@link SubscriptionResource}.
     */
    @Override
    public void onAdd(SubscriptionResource resource) {
        log.debug("Add SubscriptionResource: {}", resource);

        var partialSubscription = PartialSubscription.fromSubscriptionResource(resource);
        partialSubscriptionCache.add(partialSubscription);
    }

    /**
     * Handles the update of a {@link SubscriptionResource}.
     *
     * @param oldResource The old state of the {@link SubscriptionResource}.
     * @param newResource The new state of the {@link SubscriptionResource}.
     */
    @Override
    public void onUpdate(SubscriptionResource oldResource, SubscriptionResource newResource) {
        log.debug("Update SubscriptionResource: {}", newResource);

        var newPartialSubscription = PartialSubscription.fromSubscriptionResource(newResource);
        partialSubscriptionCache.add(newPartialSubscription);

        var oldPartialSubscription = PartialSubscription.fromSubscriptionResource(oldResource);

        log.debug("oldPartialSubscription: {} -> newPartialSubscription: {}", oldPartialSubscription, newPartialSubscription);
        if(checkIfCallbackUrlOrHttpMethodChanged(oldPartialSubscription, newPartialSubscription)
                || newPartialSubscription.isCircuitBreakerOptOut()) {
            threadPoolService.startSubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription);
        }
    }

    /**
     * Handles the deletion of a {@link SubscriptionResource}.
     *
     * @param resource               The deleted {@link SubscriptionResource}.
     * @param deletedFinalStateUnknown Indicates whether the final state is unknown.
     */
    @Override
    public void onDelete(SubscriptionResource resource, boolean deletedFinalStateUnknown) {
        log.debug("Delete SubscriptionResource: {}", resource);

        var partialSubscription = PartialSubscription.fromSubscriptionResource(resource);

        partialSubscriptionCache.remove(partialSubscription.subscriptionId());

        threadPoolService.startSubscriptionComparisonTask(partialSubscription, null);
    }

    /**
     * Handles the event when nothing has changed.
     */
    @Override
    public void onNothing() {
        ResourceEventHandler.super.onNothing();
    }



    /**
     * Checks if the callback URL or HTTP method has changed between old and new {@link PartialSubscription}.
     *
     * @param oldPartialSubscription The old state of the {@link PartialSubscription}.
     * @param newPartialSubscription The new state of the {@link PartialSubscription}.
     * @return {@code true} if either the callback URL or HTTP method has changed, otherwise {@code false}.
     */
    private boolean checkIfCallbackUrlOrHttpMethodChanged(PartialSubscription oldPartialSubscription, PartialSubscription newPartialSubscription) {
        // can be null if subscription was/is SSE
        String oldCallbackUrlOrNull = oldPartialSubscription.callbackUrl();
        String newCallbackUrlOrNull = newPartialSubscription.callbackUrl();

        boolean callbackUrlChanged = !Objects.equals(oldCallbackUrlOrNull, newCallbackUrlOrNull);
        boolean httpMethodChanged =  !Objects.equals(oldPartialSubscription.isGetMethodInsteadOfHead(), newPartialSubscription.isGetMethodInsteadOfHead());

        log.debug("callbackUrlChanged: {}", callbackUrlChanged);
        log.debug("httpMethodChanged: {}", httpMethodChanged);

        return callbackUrlChanged || httpMethodChanged;
    }
}
