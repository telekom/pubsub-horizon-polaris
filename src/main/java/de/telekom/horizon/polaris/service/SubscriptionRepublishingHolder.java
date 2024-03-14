// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.service;

import de.telekom.horizon.polaris.model.CallbackKey;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class SubscriptionRepublishingHolder {

    private final ConcurrentHashMap<CallbackKey, AtomicBoolean> subscriptionRepublishingMap = new ConcurrentHashMap<>();

    public boolean isRepublishing(CallbackKey key) {
        return subscriptionRepublishingMap.getOrDefault(key, new AtomicBoolean(false)).get();
    }

    public void startRepublishing(CallbackKey key) {
        subscriptionRepublishingMap.put(key, new AtomicBoolean(true));
    }

    public void indicateRepublishingFinished(CallbackKey key) {
        subscriptionRepublishingMap.remove(key);
    }

}
