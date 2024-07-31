// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.task;

import com.google.common.util.concurrent.ListenableScheduledFuture;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import de.telekom.horizon.polaris.util.ResultCaptor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static de.telekom.horizon.polaris.TestConstants.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    ThreadPoolService threadPoolService;


    SubscriptionComparisonTask subscriptionComparisonTask;

    @BeforeEach
    void prepare() {
        threadPoolService = MockGenerator.mockThreadPoolService();

        when(MockGenerator.messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdAsc(
                anyList(),
                eq(DeliveryType.CALLBACK),
                anyString(),
                any(Pageable.class))
        ).thenReturn(new SliceImpl<>(Collections.emptyList()));

        when(MockGenerator.workerService.tryGlobalLock()).thenReturn(true);
        when(MockGenerator.workerService.tryClaim(any())).thenReturn(true);
    }

    @Test
    @DisplayName("Clean Health Check Cache when Subscription is deleted")
    void cleanHealthCheckCacheWhenSubscriptionIsDeleted() {
        // prepare
        MockGenerator.healthCheckCache.add(CALLBACK_URL, HttpMethod.HEAD, SUBSCRIPTION_ID);
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = null;

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(MockGenerator.healthCheckCache, times(1)).remove(eq(CALLBACK_URL), eq(HttpMethod.HEAD), eq(SUBSCRIPTION_ID));
        verify(threadPoolService, never()).startHealthRequestTask(eq(CALLBACK_URL), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD));
        verify(threadPoolService, never()).startHandleDeliveryTypeChangeTask(eq(newPartialSubscription));
    }

    @Test
    @DisplayName("Only handle callbackUrls for own pod")
    void onlyHandleCallbackUrlsForOwnPod() {
        // prepare
        MockGenerator.healthCheckCache.add(CALLBACK_URL, HttpMethod.HEAD, SUBSCRIPTION_ID);
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.SERVER_SENT_EVENT, false, false);


        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "timestamp"));
        var fakeDocs = MockGenerator.createFakeMessageStateMongoDocuments(10, ENV, Status.WAITING,false);
        var fakeMessageStates = new SliceImpl<>(fakeDocs, pageable, true);

        // findByStatusWaitingOrWithCallbackExceptionAndSubscriptionIdsAndTimestampLessThanEqual ( SSE )
        when(MockGenerator.messageStateMongoRepo.findByStatusWaitingOrWithCallbackExceptionAndSubscriptionIdsAndTimestampLessThanEqual(anyList(), eq(List.of(SUBSCRIPTION_ID)), any(),  any()))
                .thenReturn( fakeMessageStates ).thenReturn(new SliceImpl<>(new ArrayList<>()));

        // findByStatusInAndDeliveryTypeAndSubscriptionIds ( CALLBACK )
        when(MockGenerator.messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdsAsc(anyList(), any(DeliveryType.class), anyList(), any()))
                .thenReturn(fakeMessageStates).thenReturn(new SliceImpl<>(new ArrayList<>()));

        when(MockGenerator.workerService.tryClaim(any())).thenReturn(false);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        //verify(MockGenerator.healthCheckCache, never()).remove(eq(CALLBACK_URL), eq(HttpMethod.HEAD), eq(SUBSCRIPTION_ID));
        //verify(threadPoolService, never()).startHandleDeliveryTypeChangeTask(eq(newPartialSubscription));
        //verify(threadPoolService, never()).startHealthRequestTask(eq(CALLBACK_URL), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD));
    }

    @Test
    @DisplayName("Handle change when Delivery Type changed from Callback to SSE")
    void handleChangeWhenChangeFromCallbackToSse() {
        // prepare
        MockGenerator.healthCheckCache.add(CALLBACK_URL, HttpMethod.HEAD, SUBSCRIPTION_ID);
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.SERVER_SENT_EVENT, false, false);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(MockGenerator.healthCheckCache, times(1)).remove(eq(CALLBACK_URL), eq(HttpMethod.HEAD), eq(SUBSCRIPTION_ID));
        verify(threadPoolService, times(1)).startHandleDeliveryTypeChangeTask(eq(newPartialSubscription));
    }

    @Test
    @DisplayName("Handle change when Delivery Type changed from SSE to Callback")
    void startHandleChangeTaskWhenChangeFromSseToCallback() {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.SERVER_SENT_EVENT, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, times(1)).startHandleDeliveryTypeChangeTask(eq(newPartialSubscription));
    }

    @Test
    @DisplayName("Start health request when nothing changed")
    void startHealthRequestWhenNothingChanged() {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, times(1)).startHealthRequestTask(eq(CALLBACK_URL), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD), any());
    }

    @Test
    @DisplayName("Start health request and clean healthCheckCache when httpMethod changed")
    void startHealthRequestAndCleanHealthCheckCacheWhenHttpMethodChanged() {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, true, false);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, times(1)).startHealthRequestTask(eq(CALLBACK_URL), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.GET), any());
        verify(MockGenerator.healthCheckCache, times(1)).remove(eq(CALLBACK_URL), eq(HttpMethod.HEAD), eq(SUBSCRIPTION_ID));
    }

    @Test
    @DisplayName("Do nothing when no circuitBreaker for new callbackUrl")
    void doNothingWhenNoCircuitBreakerForNewCallbackUrl() {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, true);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, never()).startHealthRequestTask(any(), any(), any(), any(), any());
        verify(threadPoolService, never()).startHandleDeliveryTypeChangeTask(any());
    }

    @Test
    @DisplayName("Update CircuitBreakerMessage and start health request when new callbackUrl")
    void updateCircuitBreakerMessageAndStartHealthRequestWhenNewCallbackUrl() throws ExecutionException, InterruptedException {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, true);
        var cbMessage = new CircuitBreakerMessage(SUBSCRIPTION_ID, CircuitBreakerStatus.OPEN, CALLBACK_URL, ENV);
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessage(any())).thenReturn(Optional.ofNullable(cbMessage));


        final ResultCaptor<ListenableScheduledFuture<Boolean>> resultCaptor = new ResultCaptor<>();
        doAnswer(resultCaptor).when(threadPoolService).startHealthRequestTask(eq(CALLBACK_URL_NEW), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD), any());

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, times(1)).startHealthRequestTask(eq(CALLBACK_URL_NEW), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD), any());

        Boolean wasSuccessful = resultCaptor.getResult().get();
        assertTrue(wasSuccessful);

        verify(MockGenerator.circuitBreakerCache, times(2)).updateCircuitBreakerMessage(argThat(cbM -> CALLBACK_URL_NEW.equals(cbM.getCallbackUrl())));
    }

    @Test
    @DisplayName("Cleanup cache and threads, republish when activated CircuitBreakerOptOut")
    void startHandleSuccessfulHealthRequestTaskWhenCircuitBreakerOptOut() {
        // prepare
        PartialSubscription oldPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, false, false, false);
        PartialSubscription newPartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false, true, false, false);

        subscriptionComparisonTask = new SubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription, threadPoolService);
        subscriptionComparisonTask.run();

        verify(threadPoolService, times(1)).startHandleSuccessfulHealthRequestTask(eq(CALLBACK_URL), eq(HttpMethod.HEAD));
    }
}
