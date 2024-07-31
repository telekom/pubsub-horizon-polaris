// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;


import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import static de.telekom.horizon.polaris.TestConstants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Slf4j
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CircuitBreakerManagerTest {
    ThreadPoolService threadPoolService;
    CircuitBreakerManager circuitBreakerManager;

    @BeforeEach
    void prepare() {
        threadPoolService = MockGenerator.mockThreadPoolService();

        when(MockGenerator.partialSubscriptionCache.get(anyString())).then( a -> {
            var fakePartialSubscription = new PartialSubscription(ENV, a.getArgument(0, String.class), PUBLISHER_ID, SUBSCRIBER_ID, CALLBACK_URL, DeliveryType.CALLBACK, false, false);
            return Optional.ofNullable(fakePartialSubscription);
        });

        when(MockGenerator.polarisConfig.getPollingBatchSize()).thenReturn(100);
        when(threadPoolService.getWorkerService().tryGlobalLock()).thenReturn(true);
        when(threadPoolService.getWorkerService().tryClaim(any())).thenReturn(true);

        circuitBreakerManager = spy(new CircuitBreakerManager(threadPoolService));
    }

    @Test
    void should_start_HealthRequestTask_with_delay_on_loop() {
        final var firstFakeCircuitBreakerMessage = MockGenerator.createFakeCircuitBreakerMessages(1, false, false).get(0);
        firstFakeCircuitBreakerMessage.setStatus(CircuitBreakerStatus.OPEN);

        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(0), anyInt(), eq(CircuitBreakerStatus.OPEN))).thenReturn(List.of(firstFakeCircuitBreakerMessage));
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessage(eq(SUBSCRIPTION_ID))).thenReturn(Optional.of(firstFakeCircuitBreakerMessage));

        // Create entry with open no thread and a republish count of 1
        MockGenerator.healthCheckCache.add( CALLBACK_URL, HttpMethod.HEAD, SUBSCRIPTION_ID );
        MockGenerator.healthCheckCache.update(CALLBACK_URL, HttpMethod.HEAD,401, "Unauthorized");
        MockGenerator.healthCheckCache.clearBeforeRepublishing( CALLBACK_URL, HttpMethod.HEAD); // -> sets isThreadOpen: false, republishCount: 1
        MockGenerator.healthCheckCache.incrementRepublishCount(CALLBACK_URL, HttpMethod.HEAD); // -> republishCount: 2
        MockGenerator.healthCheckCache.incrementRepublishCount(CALLBACK_URL, HttpMethod.HEAD); // -> republishCount: 3
        MockGenerator.healthCheckCache.incrementRepublishCount(CALLBACK_URL, HttpMethod.HEAD); // -> republishCount: 4
        MockGenerator.healthCheckCache.incrementRepublishCount(CALLBACK_URL, HttpMethod.HEAD); // -> republishCount: 5

        circuitBreakerManager.loadAndProcessCircuitBreakerMessages(CircuitBreakerStatus.OPEN);

        try {
            // republishCount = 1, therefore timeout is 2^5 => 32 minutes
            verify(threadPoolService, timeout(5000)).startHealthRequestTask( eq(CALLBACK_URL) , eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD), eq(Duration.of(32, ChronoUnit.MINUTES)));
            Assertions.assertTrue(true);
        } catch (Exception ex) {
            Assertions.fail(ex.getMessage());
        }
    }
}
