// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.task;

import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.component.HealthCheckRestClient;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CouldNotDetermineWorkingSetException;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.PolarisService;
import de.telekom.horizon.polaris.service.PodService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static de.telekom.horizon.polaris.TestConstants.*;
import static org.mockito.Mockito.*;

/**
 *
 *
 * @author Manuel Heß
 * @since 3.0
 */
@ExtendWith(SpringExtension.class)
@Slf4j
class SubscriptionComparisonTaskTest {

    @Mock
    ThreadPoolService threadPoolService;

    @Spy
    HealthCheckCache healthCheckCache;

    SubscriptionComparisonTask subscriptionComparisonTask;

    @Mock
    PolarisService polarisService;
    @Mock
    PodService podService;
    @Mock
    PolarisConfig polarisConfig;
    @Mock
    CircuitBreakerCacheService circuitBreakerCacheService;

    @MockBean
    MessageStateMongoRepo messageStateMongoRepo;

    @BeforeEach
    void prepare() {
        threadPoolService = MockGenerator.mockThreadPoolService();

        when(threadPoolService.getHealthCheckCache()).thenReturn(healthCheckCache);
        when(threadPoolService.getPodService()).thenReturn(podService);

        // when(threadPoolService.getPolarisService()).thenReturn(polarisService);
        when(threadPoolService.getCircuitBreakerCacheService()).thenReturn(circuitBreakerCacheService);

        when(threadPoolService.getPolarisConfig()).thenReturn(polarisConfig);
        when(polarisConfig.getPollingBatchSize()).thenReturn(10);

        when(threadPoolService.getMessageStateMongoRepo()).thenReturn(messageStateMongoRepo);
        when(messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdAsc(
                anyList(),
                eq(DeliveryType.CALLBACK),
                anyString(),
                any(Pageable.class))
        ).thenReturn(new SliceImpl<>(Collections.emptyList()));
    }

    @Test
    @DisplayName("Clean Health Check Cache when Subscription is deleted")
    void cleanHealthCheckCacheWhenSubscriptionIsDeleted() {
        // prepare
        healthCheckCache.add(CALLBACK_URL, HttpMethod.HEAD, SUBSCRIPTION_ID);
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = null;

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(healthCheckCache, times(1)).remove(eq(CALLBACK_URL), eq(HttpMethod.HEAD), eq(SUBSCRIPTION_ID));
        verify(threadPoolService, never()).startHealthRequestTask(eq(CALLBACK_URL), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD));
        verify(threadPoolService, never()).startHandleDeliveryTypeChangeTask(eq(newPartialSubscription));
    }

    @Test
    @DisplayName("Only handle callbackUrls for own pod")
    void onlyHandleCallbackUrlsForOwnPod() throws CouldNotDetermineWorkingSetException {
        // prepare
        healthCheckCache.add(CALLBACK_URL, HttpMethod.HEAD, SUBSCRIPTION_ID);
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.SERVER_SENT_EVENT, false, false);
        when(podService.shouldCallbackUrlBeHandledByThisPod(any())).thenReturn(false);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(healthCheckCache, never()).remove(eq(CALLBACK_URL), eq(HttpMethod.HEAD), eq(SUBSCRIPTION_ID));
        verify(threadPoolService, never()).startHandleDeliveryTypeChangeTask(eq(newPartialSubscription));
        verify(threadPoolService, never()).startHealthRequestTask(eq(CALLBACK_URL), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD));
    }

    @Test
    @DisplayName("Handle change when Delivery Type changed from Callback to SSE")
    void handleChangeWhenChangeFromCallbackToSse() throws CouldNotDetermineWorkingSetException {
        // prepare
        healthCheckCache.add(CALLBACK_URL, HttpMethod.HEAD, SUBSCRIPTION_ID);
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.SERVER_SENT_EVENT, false, false);
        when(podService.shouldCallbackUrlBeHandledByThisPod(any())).thenReturn(true);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(healthCheckCache, times(1)).remove(eq(CALLBACK_URL), eq(HttpMethod.HEAD), eq(SUBSCRIPTION_ID));
        verify(threadPoolService, times(1)).startHandleDeliveryTypeChangeTask(eq(newPartialSubscription));
    }

    @Test
    @DisplayName("Handle change when Delivery Type changed from SSE to Callback")
    void startHandleChangeTaskWhenChangeFromSseToCallback() throws CouldNotDetermineWorkingSetException {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.SERVER_SENT_EVENT, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        when(podService.shouldCallbackUrlBeHandledByThisPod(any())).thenReturn(true);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, times(1)).startHandleDeliveryTypeChangeTask(eq(newPartialSubscription));
    }

    @Test
    @DisplayName("Start health request when nothing changed")
    void startHealthRequestWhenNothingChanged() throws CouldNotDetermineWorkingSetException {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        when(podService.shouldCallbackUrlBeHandledByThisPod(any())).thenReturn(true);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, times(1)).startHealthRequestTask(eq(CALLBACK_URL), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD), any());
    }

    @Test
    @DisplayName("Start health request and clean healthCheckCache when httpMethod changed")
    void startHealthRequestAndCleanHealthCheckCacheWhenHttpMethodChanged() throws CouldNotDetermineWorkingSetException {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, true, false);
        when(podService.shouldCallbackUrlBeHandledByThisPod(any())).thenReturn(true);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, times(1)).startHealthRequestTask(eq(CALLBACK_URL), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.GET), any());
        verify(healthCheckCache, times(1)).remove(eq(CALLBACK_URL), eq(HttpMethod.HEAD), eq(SUBSCRIPTION_ID));
    }

    @Test
    @DisplayName("Do nothing when no circuitBreaker for new callbackUrl")
    void doNothingWhenNoCircuitBreakerForNewCallbackUrl() throws CouldNotDetermineWorkingSetException {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, true);
        circuitBreakerCacheService.closeCircuitBreaker(SUBSCRIPTION_ID);
        when(podService.shouldCallbackUrlBeHandledByThisPod(any())).thenReturn(true);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, never()).startHealthRequestTask(any(), any(), any(), any(), any());
        verify(threadPoolService, never()).startHandleDeliveryTypeChangeTask(any());
    }

    @Test
    @DisplayName("Update CircuitBreakerMessage and start health request when new callbackUrl")
    void updateCircuitBreakerMessageAndStartHealthRequestWhenNewCallbackUrl() throws CouldNotDetermineWorkingSetException {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, true);
        var cbMessage = new CircuitBreakerMessage(SUBSCRIPTION_ID, CircuitBreakerStatus.OPEN, CALLBACK_URL, ENV);
        when(circuitBreakerCacheService.getCircuitBreakerMessage(any())).thenReturn(Optional.ofNullable(cbMessage));
        when(podService.shouldCallbackUrlBeHandledByThisPod(any())).thenReturn(true);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, times(1)).startHealthRequestTask(eq(CALLBACK_URL_NEW), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD), any());
        verify(circuitBreakerCacheService, times(1)).updateCircuitBreakerMessage(argThat(cbM -> CALLBACK_URL_NEW.equals(cbM.getCallbackUrl())));
    }

    @Test
    @DisplayName("Cleanup cache and threads, republish when activated CircuitBreakerOptOut")
    void startHandleSuccessfulHealthRequestTaskWhenCircuitBreakerOptOut() throws CouldNotDetermineWorkingSetException {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, true, false, false);
        when(podService.shouldCallbackUrlBeHandledByThisPod(any())).thenReturn(true);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, times(1)).startHandleSuccessfulHealthRequestTask(eq(CALLBACK_URL), eq(HttpMethod.HEAD));
    }

}
