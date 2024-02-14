// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.cache;

import de.telekom.horizon.polaris.model.PartialSubscription;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PartialSubscriptionCache {
    private final ConcurrentHashMap<String, PartialSubscription> cache = new ConcurrentHashMap<>();

    public void add(PartialSubscription partialSubscription) {
        cache.put(partialSubscription.subscriptionId(), partialSubscription);
    }

    public void remove(String subscriptionId) {
        cache.remove(subscriptionId);
    }

    public void remove(PartialSubscription partialSubscription) {
        cache.remove(partialSubscription.subscriptionId());
    }


    public Optional<PartialSubscription> get(String subscriptionId) {
        return Optional.ofNullable(cache.get(subscriptionId));
    }

    public Optional<PartialSubscription> get(PartialSubscription partialSubscription) {
        return Optional.ofNullable(cache.get(partialSubscription.subscriptionId()));
    }
}
