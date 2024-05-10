// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.service;

import de.telekom.horizon.polaris.model.CallbackKey;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SubscriptionRepublishingHolder {

    private final Set<CallbackKey> callbackKeys = ConcurrentHashMap.newKeySet();

    public boolean isRepublishing(CallbackKey key) {
        return callbackKeys.contains(key);
    }

    public void startRepublishing(CallbackKey key) {
        callbackKeys.add(key);
    }

    public void indicateRepublishingFinished(CallbackKey key) {
        callbackKeys.remove(key);
    }

}
