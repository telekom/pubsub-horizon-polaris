// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.model;

import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerHealthCheck;
import de.telekom.horizon.polaris.cache.HealthCheckCache;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lives in the {@link HealthCheckCache} as value.
 * <br>
 * Contains:
 * <ul>
 *     <li>Thread safe Set of subscription ids</li>
 *     <li>{@link CircuitBreakerHealthCheck}</li>
 *     <li>AtomicBoolean isThreadOpen (true while HealthRequest & Republishing)</li>
 *     <li>AtomicInteger republishCount (how many time events for this entry were republished, exists to create a cooldown to reduce DUDE <---> POLARIS loops)</li>
 * </ul>
 *
 * @author Tim Pütz
 * @since 3.0
 */

@ToString
@Getter
public class HealthCheckData {
    @Getter
    private final Set<String> subscriptionIds;
    @Setter
    private CircuitBreakerHealthCheck lastHealthCheckOrNull;
    private final AtomicBoolean isThreadOpen;
    private final AtomicInteger republishCount;

    public HealthCheckData() {
        subscriptionIds = ConcurrentHashMap.newKeySet();
        lastHealthCheckOrNull = null;
        isThreadOpen = new AtomicBoolean(false);
        republishCount = new AtomicInteger(0);
    }

    public boolean isThreadOpen() {
        return isThreadOpen.get();
    }

    public int getRepublishCount() {
        return republishCount.get();
    }

    public AtomicInteger getAtomicRepublishCount() {
        return republishCount;
    }
}
