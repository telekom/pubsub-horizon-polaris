// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.cache;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PolarisPodCache {

    private final Set<String> cache = ConcurrentHashMap.newKeySet();
    public void add(String podName) {
        cache.add(podName);
    }
    public void remove(String podName) {
        cache.remove(podName);
    }
    public Set<String> getAllPods() {
        return cache;
    }
}
