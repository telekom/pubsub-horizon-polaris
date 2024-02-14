// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.service.PolarisService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import de.telekom.horizon.polaris.config.PolarisConfig;

import jakarta.annotation.PostConstruct;

/**
 * Scheduled task responsible for handling events in the WAITING state and therefore having an open circuit breaker.
 * <p>
 * This component performs the following tasks:
 * <ul>
 *   <li>During initialization, it continues working on assigned messages in the {@link CircuitBreakerManager}.</li>
 *   <li>Periodically, it loads and processes open circuit breaker messages in the {@link CircuitBreakerManager}.</li>
 * </ul>
 * The polling interval for processing open circuit breaker messages is configured in the {@link PolarisConfig}.
 * </p>
 *
 * @since 3.0
 */
@Slf4j
@Component
public class ScheduledEventWaitingHandler {
    private final PolarisService polarisService;
    private final CircuitBreakerManager circuitBreakerManager;

    public ScheduledEventWaitingHandler(PolarisService polarisService, CircuitBreakerManager circuitBreakerManager) {
        this.polarisService = polarisService;
        this.circuitBreakerManager = circuitBreakerManager;
    }

    /**
     * Continues working on assigned messages during the initialization.
     * <p>
     * If resources are fully synced, it calls {@link CircuitBreakerManager#loadAndProcessCircuitBreakerMessages(CircuitBreakerStatus)}
     * for both {@link CircuitBreakerStatus#REPUBLISHING} and {@link CircuitBreakerStatus#CHECKING}.
     * Otherwise, it sets a callback to execute the same actions once resources are fully synced.
     * </p>
     */
    @PostConstruct
    public void continueWorkingOnAssignedMessages() {
        boolean isFullySynced = polarisService.areResourcesFullySynced();
        if(isFullySynced) {
            circuitBreakerManager.loadAndProcessCircuitBreakerMessages(CircuitBreakerStatus.REPUBLISHING);
            circuitBreakerManager.loadAndProcessCircuitBreakerMessages(CircuitBreakerStatus.CHECKING);
        } else {
            polarisService.setFullySyncedCallback( () -> {
                circuitBreakerManager.loadAndProcessCircuitBreakerMessages(CircuitBreakerStatus.REPUBLISHING);
                circuitBreakerManager.loadAndProcessCircuitBreakerMessages(CircuitBreakerStatus.CHECKING);
            } );
        }
    }

    /**
     * Periodically loads and processes open circuit breaker messages.
     * <p>
     * If resources are not fully synced, it logs a message and skips processing. Otherwise
     * it calls {@link CircuitBreakerManager#loadAndProcessCircuitBreakerMessages(CircuitBreakerStatus)} for {@link CircuitBreakerStatus#OPEN}.
     * </p>
     */
    @Scheduled(fixedDelayString = "${polaris.polling.interval-ms}", initialDelayString = "${random.int(${polaris.polling.interval-ms})}")
    protected void loadAndProcessOpenCircuitBreakerMessagesScheduled() {
        log.info("Start ScheduledEventWaitingHandler");
        boolean isFullySynced = polarisService.areResourcesFullySynced();
        if(!isFullySynced) {
            log.info("Resources are not fully synced yet. Waiting for next run...");
            return;
        }
        circuitBreakerManager.loadAndProcessCircuitBreakerMessages(CircuitBreakerStatus.OPEN);
        log.info("Finished ScheduledEventWaitingHandler");
    }




}
