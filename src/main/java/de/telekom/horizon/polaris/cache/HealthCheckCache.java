// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.cache;

import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerHealthCheck;
import de.telekom.horizon.polaris.model.CallbackKey;
import de.telekom.horizon.polaris.model.HealthCheckData;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache from (CallbackUrl-Environment-HttpMethod) to subscriptionIds and healthCheck
 * We use this key combination to later on have a Request task for each key of this map.
 * The alternative would have been to just use the callbackUrl as key, but then we would have to put a lot of logic
 * into the RequestTask (which should be rather stupid).
 */
@Component
@Slf4j
public class HealthCheckCache {
    @Getter(AccessLevel.PRIVATE)
    private final ConcurrentHashMap<CallbackKey, HealthCheckData> cache;

    public HealthCheckCache() {
        this.cache = new ConcurrentHashMap<>();
    }

    public Enumeration<CallbackKey> getAllKeys() {
        return getCache().keys();
    }
    public Collection<HealthCheckData> getAll() {
        return getCache().values();
    }

    /**
     * Adds a subscription ID to the cache for the specified callback URL, HTTP method, and subscription ID.
     *
     * @param callbackUrl     The callback URL.
     * @param httpMethod       The HTTP method.
     * @param subscriptionId   The subscription ID to be added.
     * @return True if the addition was successful, false otherwise.
     */
    public boolean add(String callbackUrl, HttpMethod httpMethod, String subscriptionId) {
        return add(callbackUrl, httpMethod, List.of(subscriptionId));
    }

    /**
     * Adds multiple subscription IDs to the cache for the specified callback URL, HTTP method, and subscription IDs.
     *
     * @param callbackUrl       The callback URL.
     * @param httpMethod         The HTTP method.
     * @param subscriptionIds   The list of subscription IDs to be added.
     * @return True if the addition was successful, false otherwise.
     */
    public boolean add(String callbackUrl, HttpMethod httpMethod, List<String> subscriptionIds) {
        log.debug("add subscriptionIds: {} to callbackUrl {} and httpMethod: {}", subscriptionIds, callbackUrl, httpMethod);
        if(StringUtils.isBlank(callbackUrl)) {
            return false;
        }

        String trimmedCallbackUrl = callbackUrl.trim();
        CallbackKey key = new CallbackKey(trimmedCallbackUrl, httpMethod);

        var newHealthCheckData = getCache().compute(key, (callbackKey, oldHealthCheckDataOrNull) -> {
            log.debug("Computing add for callbackUrl {} and httpMethod: {}. oldHealthCheckDataOrNull: {}", callbackUrl, httpMethod, oldHealthCheckDataOrNull);
            HealthCheckData healthCheckData = new HealthCheckData();
            healthCheckData.setKey(key.toString());
            if(oldHealthCheckDataOrNull != null) {
                healthCheckData = oldHealthCheckDataOrNull;
            }

            healthCheckData.getSubscriptionIds().addAll(subscriptionIds);
            return healthCheckData;
        });

        boolean wasThreadNotOpenAlready = newHealthCheckData.getIsThreadOpen().compareAndSet(false, true);

        log.debug("added entry for callbackUrl {} and httpMethod: {}. Could set isThreadOpen from false to true?: {}, Entry now: {}", callbackUrl, httpMethod, wasThreadNotOpenAlready, newHealthCheckData);
        return wasThreadNotOpenAlready;
    }


    private Optional<HealthCheckData> get(CallbackKey key) {
        return Optional.ofNullable(getCache().get(key));
    }
    /**
     * Retrieves an optional HealthCheckData for the specified callback URL and HTTP method.
     *
     * @param callbackUrl The callback URL.
     * @param httpMethod   The HTTP method.
     * @return An Optional containing the HealthCheckData if found, or empty otherwise.
     */
    public Optional<HealthCheckData> get(String callbackUrl, HttpMethod httpMethod) {
        return StringUtils.isNotBlank(callbackUrl)
                ? get(new CallbackKey(callbackUrl.trim(), httpMethod))
                : Optional.empty();
    }

    /**
     * Retrieves a list of subscription IDs for the specified callback URL and HTTP method.
     *
     * @param callbackUrl The callback URL.
     * @param httpMethod   The HTTP method.
     * @return List of subscription IDs, or an empty list if not found.
     */
    public List<String> getSubscriptionIds(String callbackUrl, HttpMethod httpMethod) {
        var optional = get(callbackUrl, httpMethod);
        return optional.map(healthCheckData -> healthCheckData.getSubscriptionIds().stream().toList()).orElseGet(ArrayList::new);
    }

    /**
     * Removes the HealthCache entry for the specified callback URL and HTTP method.
     *
     * @param callbackUrl The callback URL.
     * @param httpMethod   The HTTP method.
     * @return The removed HealthCheckData if found, otherwise null.
     */
    public HealthCheckData remove(String callbackUrl, HttpMethod httpMethod) {
        log.debug("remove entry for callbackUrl {} and httpMethod: {}", callbackUrl, httpMethod);
        if(StringUtils.isBlank(callbackUrl)) {
            return null;
        }

        String trimmedCallbackUrl = callbackUrl.trim();
        CallbackKey key = new CallbackKey(trimmedCallbackUrl, httpMethod);
        return getCache().remove(key);
    }

    /**
     * Removes the subscriptionId from the HealthCache entry (if existing).
     *
     * @param subscriptionId to be removed
     */
    public void remove(String callbackUrl, HttpMethod httpMethod, String subscriptionId) {
        remove(callbackUrl, httpMethod, Set.of(subscriptionId));
    }

    /**
     * Removes the subscriptionIds from the HealthCache entry (if existing).
     *
     * @param subscriptionIds to be removed
     */
    public void remove(String callbackUrl, HttpMethod httpMethod, List<String> subscriptionIds) {
        remove(callbackUrl, httpMethod, new HashSet<>(subscriptionIds));
    }

    /**
     * Removes the subscriptionIds from the HealthCache entry (if existing).
     *
     * @param subscriptionIds to be removed
     */
    public void remove(String callbackUrl, HttpMethod httpMethod, Set<String> subscriptionIds) {
        log.debug("remove subscriptionIds: {} from callbackUrl {} and httpMethod: {}", subscriptionIds, callbackUrl, httpMethod);
        if(StringUtils.isBlank(callbackUrl)) { return; }

        String trimmedCallbackUrl = callbackUrl.trim();
        CallbackKey key = new CallbackKey(trimmedCallbackUrl, httpMethod);
        var newHealthCheckData = getCache().computeIfPresent(key, (callbackKey, oldHealthCheckData) -> {
            log.debug("Entry found for callbackUrl {} and httpMethod: {}. OldHealthCheckData: {}", callbackUrl, httpMethod, oldHealthCheckData);
            oldHealthCheckData.getSubscriptionIds().removeAll(subscriptionIds);
            return oldHealthCheckData;
        });

        log.debug("removed subscriptionIds: {} from callbackUrl {} and httpMethod: {}. Entry now: {}", subscriptionIds, callbackUrl, httpMethod, newHealthCheckData);
    }


    /**
     * Removes all subscriptionIds from the found entry and returns them.
     * Additionally, sets the isThreadOpen to false and increments the republishCount.
     * <br><br>
     * Does <strong>not</strong> end the corresponding thread!
     *
     * @return subscriptionIds that were removed from the HealthCheck entry
     */
    public List<String> clearBeforeRepublishing(String callbackUrl, HttpMethod httpMethod) {
        return clearBeforeRepublishing(callbackUrl, httpMethod, List.of());
    }

    /**
     * Removes given subscriptionIds from the found entry and returns them.
     * Additionally, sets the isThreadOpen to false and increments the republishCount.
     * <br><br>
     * Does <strong>not</strong> end the corresponding thread!
     *
     * @param subscriptionIdsToRemove Leave empty to remove all subscriptionIds
     * @return subscriptionIds that were removed from the HealthCheck entry
     */
    public List<String> clearBeforeRepublishing(String callbackUrl, HttpMethod httpMethod, List<String> subscriptionIdsToRemove) {
        log.debug("clearBeforeRepublishing from callbackUrl {} and httpMethod: {}", callbackUrl, httpMethod);
        if(StringUtils.isBlank(callbackUrl)) { return new ArrayList<>(); }

        String trimmedCallbackUrl = callbackUrl.trim();
        CallbackKey key = new CallbackKey(trimmedCallbackUrl, httpMethod);

        var removedSubscriptionIds = new ArrayList<String>();
        var newHealthCheckData = getCache().computeIfPresent(key, (callbackKey, oldHealthCheckData) -> {
            log.debug("Entry found for callbackUrl {} and httpMethod: {}. OldHealthCheckData: {}", callbackUrl, httpMethod, oldHealthCheckData);
            var innerSubscriptionIds = oldHealthCheckData.getSubscriptionIds();
            if(subscriptionIdsToRemove.isEmpty()) {
                removedSubscriptionIds.addAll(innerSubscriptionIds);
                oldHealthCheckData.getSubscriptionIds().clear();
            } else {
                removedSubscriptionIds.addAll(subscriptionIdsToRemove);
                subscriptionIdsToRemove.forEach(oldHealthCheckData.getSubscriptionIds()::remove);
            }

            if(oldHealthCheckData.getSubscriptionIds().isEmpty()) {
                oldHealthCheckData.getIsThreadOpen().set(false);
                oldHealthCheckData.getAtomicRepublishCount().incrementAndGet();
            }

            return oldHealthCheckData;
        });

        log.debug("clearedBeforeRepublishing from callbackUrl {} and httpMethod: {}. Entry now: {}", callbackUrl, httpMethod, newHealthCheckData);
        return removedSubscriptionIds;
    }

    /**
     * Updates the healthCheck field to the specified CircuitBreakerHealthCheck.
     *
     * @param callbackUrl       The callback URL.
     * @param httpMethod         The HTTP method.
     * @param healthCheck        The CircuitBreakerHealthCheck to be set.
     * @return The updated HealthCheckData if found, otherwise null.
     */
    public HealthCheckData update(String callbackUrl, HttpMethod httpMethod, CircuitBreakerHealthCheck healthCheck) {
        log.debug("updating healthCheck to {} for callbackUrl {} and httpMethod: {}", healthCheck, callbackUrl, httpMethod);
        var key = new CallbackKey(callbackUrl, httpMethod);
        var newHealthCheckData = getCache().computeIfPresent( key, (callbackKey, oldHealthCheckData) -> {
            log.debug("Entry found for callbackUrl {} and httpMethod: {}. OldHealthCheckData: {}", callbackUrl, httpMethod, oldHealthCheckData);
            oldHealthCheckData.setLastHealthCheckOrNull(healthCheck);
            return oldHealthCheckData;
        });
        log.debug("updated healthCheck to {} from callbackUrl {} and httpMethod: {}. Entry now: {}", healthCheck, callbackUrl, httpMethod, newHealthCheckData);
        return newHealthCheckData;
    }

    /**
     * Updates the HealthCheckData object with the specified status code and reason phrase for the given callback URL and HTTP method.
     *
     * @param callbackUrl   The callback URL.
     * @param httpMethod     The HTTP method.
     * @param statusCode     The status code to be updated.
     * @param reasonPhrase   The reason phrase to be updated.
     * @return HealthCheckData object after the update.
     */
    public HealthCheckData update(String callbackUrl, HttpMethod httpMethod, int statusCode, String reasonPhrase) {
        log.debug("updating healthCheck by statusCode ({}) and reasonPhrase ({}) for callbackUrl {} and httpMethod: {}", statusCode, reasonPhrase, callbackUrl, httpMethod);
        var key = new CallbackKey(callbackUrl, httpMethod);
        var newHealthCheckData = getCache().computeIfPresent( key, (callbackKey, oldHealthCheckData) -> {

            var newHealthCheck = new CircuitBreakerHealthCheck(Date.from(Instant.now()), Date.from(Instant.now()), statusCode, reasonPhrase);
            if (oldHealthCheckData.getLastHealthCheckOrNull() != null) {
                log.debug("Entry found for callbackUrl {} and httpMethod: {}. OldHealthCheckData: {}", callbackUrl, httpMethod, oldHealthCheckData);
                var lastHealthCheck = oldHealthCheckData.getLastHealthCheckOrNull();
                newHealthCheck = new CircuitBreakerHealthCheck(lastHealthCheck.getFirstCheckedDate(), Date.from(Instant.now()), statusCode, reasonPhrase);
            }

            oldHealthCheckData.setLastHealthCheckOrNull(newHealthCheck);

            return oldHealthCheckData;
        });
        log.debug("updated healthCheck by statusCode ({}) and reasonPhrase ({}) for callbackUrl {} and httpMethod: {}. Entry now: {}", statusCode, reasonPhrase, callbackUrl, httpMethod, newHealthCheckData);
        return newHealthCheckData;
    }

    /**
     * Updates the isThreadOpen attribute for the specified callback URL and HTTP method.
     *
     * @param callbackUrl The callback URL.
     * @param httpMethod   The HTTP method.
     * @param isThreadOpen The boolean value to set for isThreadOpen.
     * @return HealthCheckData object after the update.
     */
    public HealthCheckData update(String callbackUrl, HttpMethod httpMethod, boolean isThreadOpen) {
        log.debug("updating isThreadOpen to {} for callbackUrl {} and httpMethod: {}", isThreadOpen, callbackUrl, httpMethod);
        var key = new CallbackKey(callbackUrl, httpMethod);
        var newHealthCheckData = getCache().computeIfPresent( key, (callbackKey, oldHealthCheckData) -> {
            log.debug("Entry found for callbackUrl {} and httpMethod: {}. OldHealthCheckData: {}", callbackUrl, httpMethod, oldHealthCheckData);
            oldHealthCheckData.getIsThreadOpen().set(isThreadOpen);
            return oldHealthCheckData;
        });
        log.debug("updated isThreadOpen to {} for callbackUrl {} and httpMethod: {}", isThreadOpen, callbackUrl, httpMethod);
        return newHealthCheckData;
    }

    /**
     * Increments the republish count for the specified callback URL and HTTP method.
     *
     * @param callbackUrl The callback URL.
     * @param httpMethod   The HTTP method.
     * @return HealthCheckData object after incrementing the republish count.
     */
    public HealthCheckData incrementRepublishCount(String callbackUrl, HttpMethod httpMethod) {
        log.debug("incrementing republish count for callbackUrl {} and httpMethod: {}", callbackUrl, httpMethod);
        var key = new CallbackKey(callbackUrl, httpMethod);
        var newHealthCheckData = getCache().computeIfPresent( key, (callbackKey, oldHealthCheckData) -> {
            log.debug("Entry found for callbackUrl {} and httpMethod: {}. OldHealthCheckData: {}", callbackUrl, httpMethod, oldHealthCheckData);
            oldHealthCheckData.getAtomicRepublishCount().incrementAndGet();
            return oldHealthCheckData;
        });
        log.debug("incremented republish count for callbackUrl {} and httpMethod: {}", callbackUrl, httpMethod);
        return newHealthCheckData;
    }

    /**
     * Resets the republish count to zero for the specified callback URL and HTTP method.
     *
     * @param callbackUrl The callback URL.
     * @param httpMethod   The HTTP method.
     * @return HealthCheckData object after resetting the republish count.
     */
    public HealthCheckData resetRepublishCount(String callbackUrl, HttpMethod httpMethod) {
        log.debug("reset republish count for callbackUrl {} and httpMethod: {}", callbackUrl, httpMethod);
        var key = new CallbackKey(callbackUrl, httpMethod);
        var newHealthCheckData = getCache().computeIfPresent( key, (callbackKey, oldHealthCheckData) -> {
            log.debug("Entry found for callbackUrl {} and httpMethod: {}. OldHealthCheckData: {}", callbackUrl, httpMethod, oldHealthCheckData);
            oldHealthCheckData.getAtomicRepublishCount().set(0);
            return oldHealthCheckData;
        });
        log.debug("reseted republish count for callbackUrl {} and httpMethod: {}", callbackUrl, httpMethod);
        return newHealthCheckData;
    }
}
