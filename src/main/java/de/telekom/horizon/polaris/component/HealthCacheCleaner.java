// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerHealthCheck;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.model.CallbackKey;
import de.telekom.horizon.polaris.model.HealthCheckData;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Loops every 30 minutes through the health cache and deletes old entries or dead/invalid entries.
 * <br>
 * An old entry is an entry with last health check date <= (now - requestCooldownResetMins)
 *
 * @author Tim Pütz
 * @since 3.0
 */
@Slf4j
@Component
public class HealthCacheCleaner {
    private final HealthCheckCache healthCheckCache;
    private final PolarisConfig polarisConfig;
    private final ThreadPoolService threadPoolService;

    private final CircuitBreakerCacheService circuitBreakerCacheService;

    public HealthCacheCleaner(ThreadPoolService threadPoolService) {
        this.healthCheckCache = threadPoolService.getHealthCheckCache();
        this.polarisConfig = threadPoolService.getPolarisConfig();
        this.circuitBreakerCacheService = threadPoolService.getCircuitBreakerCacheService();
        this.threadPoolService = threadPoolService;
    }

    /**
     * Scheduled task to clean the health cache by removing old or invalid entries.
     * This task runs every 30 minutes or configured otherwise.
     * <p>
     * For each entry in the health cache, it checks if the last health check date is older than the configured
     * {@link PolarisConfig#getRequestCooldownResetMins()}. If so, it removes the entry from the health cache, stops the associated
     * health request task, and opens the circuit breakers for the corresponding subscriptions.
     * </p>
     *
     */
    @Scheduled(fixedDelay = 30, timeUnit = TimeUnit.MINUTES)
    public void cleanHealthCache() {
        log.info("Start cleanHealthCache");
        var keys = Collections.list(healthCheckCache.getAllKeys());

        log.debug("cleanHealthCache keys: {}", keys);
        for(var key : keys) {
            var oHealthCheckData = healthCheckCache.get(key.callbackUrl(), key.httpMethod());
            if(oHealthCheckData.isEmpty()) { continue; }

            var healthCheckData = oHealthCheckData.get();
            var lastHealthCheckOrNull = healthCheckData.getLastHealthCheckOrNull();

            boolean isDeletableByTime = checkIfDeletableByTime(lastHealthCheckOrNull);

            if(isDeletableByTime) {
                if(healthCheckData.isThreadOpen()) {
                    stopHealthRequestTask(key);
                    openCircuitBreakers(healthCheckData.getSubscriptionIds());
                }

                removeEntry(key);
            }
        }
        log.info("Finished cleanHealthCache");
    }

    /**
     * Opens circuit breakers associated with the given subscription IDs.
     *
     * @param subscriptionIds The subscription IDs to open circuit breakers for.
     */
    private void openCircuitBreakers(Set<String> subscriptionIds) {
        for (var subscriptionId : subscriptionIds) {
            log.info("cleanHealthCache updateCircuitBreakerStatus for subscriptionId: {}", subscriptionId);
            circuitBreakerCacheService.updateCircuitBreakerStatus(subscriptionId, CircuitBreakerStatus.OPEN);
        }
    }

    /**
     * Stops the health request task associated with the given callback key.
     *
     * @param key The callback key to stop the health request task for.
     */
    private void stopHealthRequestTask(CallbackKey key) {
        var oFuture = threadPoolService.getHealthRequestTask(key.callbackUrl(), key.httpMethod());
        if (oFuture.isPresent()) {
            log.info("cleanHealthCache stopHealthRequestTask for callbackUrl: {} and httpMethod: {}", key.callbackUrl(), key.httpMethod());
            threadPoolService.stopHealthRequestTask(key.callbackUrl(), key.httpMethod());
        }
    }

    /**
     * Removes the entry associated with the given callback key from the health cache.
     *
     * @param key The callback key to remove the entry for.
     */
    private void removeEntry(CallbackKey key) {
        var cleanedUpEntry = healthCheckCache.remove(key.callbackUrl(), key.httpMethod());
        log.info("Cleaned up entry with key: {} and value: {}", key, cleanedUpEntry);
    }

    /**
     * Checks if the entry is deletable based on the last health check date.
     *
     * @param lastHealthCheckOrNull The last health check date or null if not available.
     * @return True if the entry is deletable, false otherwise.
     */
    private boolean checkIfDeletableByTime(CircuitBreakerHealthCheck lastHealthCheckOrNull) {
        if(lastHealthCheckOrNull != null && lastHealthCheckOrNull.getLastCheckedDate() != null) {
            var duration = Duration.between(Instant.now(), lastHealthCheckOrNull.getLastCheckedDate().toInstant()).abs();
            return duration.toMinutes() >= polarisConfig.getRequestCooldownResetMins();
        }

        return false;
    }
}
