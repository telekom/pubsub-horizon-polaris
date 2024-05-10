// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.exception.CouldNotDetermineWorkingSetException;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.Optional;

import static de.telekom.horizon.polaris.TestConstants.*;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Slf4j
class ScheduledEventWaitingHandlerTest {

    ThreadPoolService threadPoolService;

    CircuitBreakerManager circuitBreakerManager;

    ScheduledEventWaitingHandler scheduledEventWaitingHandler;

    CircuitBreakerMessage fakeCircuitBreakerMessage;
    PartialSubscription fakePartialSubscription;

    @BeforeEach
    void prepare() {
        log.info("prepare");

        threadPoolService = MockGenerator.mockThreadPoolService();

        fakeCircuitBreakerMessage = new CircuitBreakerMessage(SUBSCRIPTION_ID, CircuitBreakerStatus.OPEN, CALLBACK_URL, ENV);

        fakePartialSubscription = new PartialSubscription(ENV, SUBSCRIPTION_ID, PUBLISHER_ID, SUBSCRIBER_ID, CALLBACK_URL, DeliveryType.CALLBACK, false, false);

        circuitBreakerManager = spy(new CircuitBreakerManager(threadPoolService));

        when(MockGenerator.partialSubscriptionCache.get(eq(SUBSCRIPTION_ID))).thenReturn( Optional.ofNullable(fakePartialSubscription) );

        scheduledEventWaitingHandler = new ScheduledEventWaitingHandler(circuitBreakerManager);

        when(threadPoolService.getWorkerService().tryGlobalLock()).thenReturn(true);
        when(threadPoolService.getWorkerService().tryClaim(any())).thenReturn(true);
    }

    @Test
    @DisplayName("should start RepublishingSubscription- & start SubscriptionComparisonTask")
    void shouldStartRepublishingAndSubscriptionComparisonTask() {
        var fakeCheckingCbMessage = SerializationUtils.clone(fakeCircuitBreakerMessage);
        fakeCheckingCbMessage.setStatus(CircuitBreakerStatus.CHECKING);
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(0), anyInt(), eq(CircuitBreakerStatus.CHECKING))).thenReturn( List.of(fakeCheckingCbMessage));

        var fakeRepublishingCbMessage = SerializationUtils.clone(fakeCircuitBreakerMessage);
        fakeRepublishingCbMessage.setStatus(CircuitBreakerStatus.REPUBLISHING);

        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(0), anyInt(), eq(CircuitBreakerStatus.REPUBLISHING))).thenReturn( List.of(fakeRepublishingCbMessage));
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(1), anyInt(), any(CircuitBreakerStatus.class))).thenReturn( List.of() );

        scheduledEventWaitingHandler.continueWorkingOnAssignedMessages();
        verify(MockGenerator.circuitBreakerCache, times(1)).getCircuitBreakerMessages(anyInt(), anyInt(), eq(CircuitBreakerStatus.CHECKING) );
        verify(MockGenerator.circuitBreakerCache, times(1)).getCircuitBreakerMessages(anyInt(), anyInt(), eq(CircuitBreakerStatus.REPUBLISHING) );
        

        verify(threadPoolService, times(2)).startSubscriptionComparisonTask(notNull(), eq(fakePartialSubscription));
    }

    @Test
    @DisplayName("should start SubscriptionComparisonTask on scheduled run")
    void shouldStartSubscriptionComparisonTask() {

        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(0), anyInt(), any(CircuitBreakerStatus.class))).thenReturn( List.of(fakeCircuitBreakerMessage));
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(1), anyInt(), any(CircuitBreakerStatus.class))).thenReturn( List.of() );

        // Normally gets called every 30 secs
        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();
        verify(MockGenerator.circuitBreakerCache, times(1)).getCircuitBreakerMessages(anyInt(), anyInt(), eq(CircuitBreakerStatus.OPEN) ); // 2 times, bc 2 pages

        verify(threadPoolService, times(1)).startSubscriptionComparisonTask(notNull(), eq(fakePartialSubscription));
    }

    @Test
    @DisplayName("should not start SubscriptionComparisonTask when no subscription found")
    void shouldNotStartSubscriptionComparisonTaskWhenNoSubscriptionFound() {
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(0), anyInt(), any(CircuitBreakerStatus.class))).thenReturn( List.of(fakeCircuitBreakerMessage));
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(1), anyInt(), any(CircuitBreakerStatus.class))).thenReturn( List.of() );


        when(MockGenerator.partialSubscriptionCache.get(eq(SUBSCRIPTION_ID))).thenReturn( Optional.empty() );

        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();

        verify(MockGenerator.circuitBreakerCache, times(1)).getCircuitBreakerMessages(anyInt(), anyInt(), eq(CircuitBreakerStatus.OPEN) ); // 2 times, bc 2 pages

        // never, bc broke out of function before
        verify(MockGenerator.circuitBreakerCache, never()).getCircuitBreakerMessage(eq(SUBSCRIPTION_ID));
        verify(threadPoolService, never()).startSubscriptionComparisonTask(notNull(), eq(fakePartialSubscription));
    }

    @Test
    @DisplayName("should not start SubscriptionComparisonTask when pod should not handle open circuit breaker message")
    void shouldNotStartSubscriptionComparisonTaskWhenPodShouldNotHandleOpenCircuitBreakerMessage() throws CouldNotDetermineWorkingSetException {
        // with circuit breaker status OPEN
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(0), anyInt(), any(CircuitBreakerStatus.class))).thenReturn( List.of(fakeCircuitBreakerMessage));
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(1), anyInt(), any(CircuitBreakerStatus.class))).thenReturn( List.of() );

        when(threadPoolService.getWorkerService().tryClaim(any())).thenReturn(false);

        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();

        verify(MockGenerator.circuitBreakerCache, times(1)).getCircuitBreakerMessages(anyInt(), anyInt(), eq(CircuitBreakerStatus.OPEN) ); // 2 times, bc 2 pages

        // never, bc broke out of function before
        verify(MockGenerator.circuitBreakerCache, never()).getCircuitBreakerMessage(eq(SUBSCRIPTION_ID));
        verify(threadPoolService, never()).startSubscriptionComparisonTask(notNull(), eq(fakePartialSubscription));
    }

    @ParameterizedTest
    @EnumSource(value = CircuitBreakerStatus.class, mode = EnumSource.Mode.EXCLUDE, names = { "OPEN" })
    @DisplayName("should not start SubscriptionComparisonTask when pod should not handle checking or republished circuit breaker message")
    void shouldNotStartSubscriptionComparisonTaskWhenPodShouldNotHandleUnopenCircuitBreakerMessage(CircuitBreakerStatus circuitBreakerStatus) throws CouldNotDetermineWorkingSetException {
        // with circuit breaker status OPEN

        var fakeCircuitBreakerMessageClone = SerializationUtils.clone(fakeCircuitBreakerMessage);
        fakeCircuitBreakerMessageClone.setStatus(circuitBreakerStatus);

        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(0), anyInt(), any(CircuitBreakerStatus.class))).thenReturn( List.of(fakeCircuitBreakerMessageClone));
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(1), anyInt(), any(CircuitBreakerStatus.class))).thenReturn( List.of() );
        when(threadPoolService.getWorkerService().tryClaim(any())).thenReturn(false);

        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();

        verify(MockGenerator.circuitBreakerCache, times(1)).getCircuitBreakerMessages(anyInt(), anyInt(), eq(CircuitBreakerStatus.OPEN) ); // 2 times, bc 2 pages

        // never, bc broke out of function before
        verify(MockGenerator.circuitBreakerCache, never()).getCircuitBreakerMessage(eq(SUBSCRIPTION_ID));
        verify(threadPoolService, never()).startSubscriptionComparisonTask(notNull(), eq(fakePartialSubscription));
    }
}

