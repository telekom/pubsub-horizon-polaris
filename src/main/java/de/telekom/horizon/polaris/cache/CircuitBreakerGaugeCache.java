// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.cache;

import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.METRIC_OPEN_CIRCUIT_BREAKERS;
import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.TAG_SUBSCRIPTION_ID;

@Component
public class CircuitBreakerGaugeCache {

    private final HorizonMetricsHelper metricsHelper;
    private final ConcurrentHashMap<String, AtomicInteger> cache;

    public CircuitBreakerGaugeCache(HorizonMetricsHelper metricsHelper) {
        this.metricsHelper = metricsHelper;
        this.cache = new ConcurrentHashMap<>();
    }

    public AtomicInteger getOrCreateGaugeForSubscription(String subscriptionId) {
        cache.computeIfAbsent(subscriptionId, s -> createGaugeForSubscription(subscriptionId));

        return cache.get(subscriptionId);
    }

    private Tags buildTagsForCircuitBreaker(String subscriptionId) {
        return Tags.of(
                TAG_SUBSCRIPTION_ID, subscriptionId
        );
    }

    private AtomicInteger createGaugeForSubscription(String subscriptionId) {
        var tags = buildTagsForCircuitBreaker(subscriptionId);

        return metricsHelper.getRegistry().gauge(METRIC_OPEN_CIRCUIT_BREAKERS, tags, new AtomicInteger(0));
    }
}
