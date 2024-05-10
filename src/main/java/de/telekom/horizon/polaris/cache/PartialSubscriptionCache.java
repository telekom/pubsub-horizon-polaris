// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.cache;

import de.telekom.eni.pandora.horizon.cache.service.JsonCacheService;
import de.telekom.eni.pandora.horizon.exception.JsonCacheException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.polaris.model.PartialSubscription;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
public class PartialSubscriptionCache {

    private final JsonCacheService<SubscriptionResource> cache;

    public PartialSubscriptionCache(JsonCacheService<SubscriptionResource> cache) {
        this.cache = cache;
    }

    public Optional<PartialSubscription> get(String subscriptionId) {
        try {
            return cache.getByKey(subscriptionId).map(PartialSubscription::fromSubscriptionResource);
        } catch (JsonCacheException e) {
            log.error("Could not get SubscriptionId from cache", e);
        }

        return Optional.empty();
    }

    public Optional<PartialSubscription> get(PartialSubscription partialSubscription) throws JsonCacheException {
        return get(partialSubscription.subscriptionId());
    }
}
