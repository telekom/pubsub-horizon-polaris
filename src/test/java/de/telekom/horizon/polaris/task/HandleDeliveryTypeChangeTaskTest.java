// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.task;


import com.fasterxml.jackson.core.JsonProcessingException;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static de.telekom.horizon.polaris.TestConstants.ENV;
import static de.telekom.horizon.polaris.TestConstants.SUBSCRIPTION_ID;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 *
 * @author Tim Pütz
 * @since 3.0
 */
@ExtendWith(SpringExtension.class)
@Slf4j
@DisplayNameGeneration(value = ReplaceUnderscores.class)
class HandleDeliveryTypeChangeTaskTest {
    ThreadPoolService threadPoolService;

    HandleDeliveryTypeChangeTask handleDeliveryTypeChangeTask;

    @BeforeEach
    void prepare() {
       threadPoolService = MockGenerator.mockThreadPoolService();

        var fakeCircuitBreakerMessage = MockGenerator.createFakeCircuitBreakerMessages(1).get(0);
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessage( eq(SUBSCRIPTION_ID) )).thenReturn(Optional.ofNullable(fakeCircuitBreakerMessage));

        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "timestamp"));
        var fakeDocs = MockGenerator.createFakeMessageStateMongoDocuments(10, ENV, Status.WAITING,false);
        var fakeMessageStates = new SliceImpl<>(fakeDocs, pageable, true);

        when(MockGenerator.messageStateMongoRepo.findByStatusWaitingOrFailedWithCallbackExceptionAndSubscriptionIdsAndTimestampLessThanEqual(anyList(), eq(List.of(SUBSCRIPTION_ID)), any(),  any()))
                .thenReturn( fakeMessageStates ).thenReturn(new SliceImpl<>(new ArrayList<>()));

        when(MockGenerator.messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdsAsc(anyList(), any(DeliveryType.class), anyList(), any()))
                .thenReturn(fakeMessageStates).thenReturn(new SliceImpl<>(new ArrayList<>()));


    }

    @Test
    void should_call_FindByStatusInAndDeliveryTypeAndSubscriptionIds() {
        var fakePartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.CALLBACK, false);
        handleDeliveryTypeChangeTask = new HandleDeliveryTypeChangeTask( fakePartialSubscription, threadPoolService );

        handleDeliveryTypeChangeTask.run();

        verify(MockGenerator.messageStateMongoRepo, times(2))
                .findByStatusInAndDeliveryTypeAndSubscriptionIdsAsc( anyList(), any(DeliveryType.class), eq(List.of(SUBSCRIPTION_ID)), any(Pageable.class));
        verify(MockGenerator.messageStateMongoRepo, never()).
                findByStatusWaitingOrFailedWithCallbackExceptionAndSubscriptionIdsAndTimestampLessThanEqual( anyList(), anyList(), any(), any(Pageable.class));
    }

    @Test
    void should_not_call_FindByStatusInAndDeliveryTypeAndSubscriptionIds() {
        var fakePartialSubscription = MockGenerator.createFakePartialSubscription(DeliveryType.SERVER_SENT_EVENT, false);
        handleDeliveryTypeChangeTask = new HandleDeliveryTypeChangeTask( fakePartialSubscription, threadPoolService );

        handleDeliveryTypeChangeTask.run();

        verify(MockGenerator.messageStateMongoRepo, never()).findByStatusInAndDeliveryTypeAndSubscriptionIdsAsc( anyList(), any(DeliveryType.class), eq(List.of(SUBSCRIPTION_ID)), any(Pageable.class));
        verify(MockGenerator.messageStateMongoRepo, times(2)).findByStatusWaitingOrFailedWithCallbackExceptionAndSubscriptionIdsAndTimestampLessThanEqual( anyList(), anyList(), any(), any(Pageable.class));
    }

    @ParameterizedTest
    @EnumSource(DeliveryType.class)
    void should_set_CircuitBreaker_to_REPUBLISH_and_close_afterwards(DeliveryType deliveryType) throws JsonProcessingException {

        when(MockGenerator.kafkaTemplate.receive(any(), anyInt(), anyLong(), any())).thenReturn( MockGenerator.createFakeConsumerRecord(deliveryType) );

        var fakePartialSubscription = MockGenerator.createFakePartialSubscription(deliveryType, false);
        handleDeliveryTypeChangeTask = new HandleDeliveryTypeChangeTask( fakePartialSubscription, threadPoolService );

        handleDeliveryTypeChangeTask.run();

        // Set to REPUBLISHING
        verify(MockGenerator.circuitBreakerCache, times(1)).getCircuitBreakerMessage(anyString());
        verify(MockGenerator.circuitBreakerCache, times(1)).updateCircuitBreakerStatus( eq(SUBSCRIPTION_ID), eq(CircuitBreakerStatus.REPUBLISHING) );

        // Close
        verify(MockGenerator.circuitBreakerCache, times(1)).closeCircuitBreaker(eq(SUBSCRIPTION_ID));
    }
}
