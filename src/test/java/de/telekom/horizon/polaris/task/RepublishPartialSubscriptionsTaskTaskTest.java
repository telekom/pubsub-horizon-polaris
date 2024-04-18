// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.model.HealthCheckData;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static de.telekom.horizon.polaris.TestConstants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 *
 * @author Tim Pütz
 * @since 3.0
 */
@ExtendWith(SpringExtension.class)
@Slf4j
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RepublishPartialSubscriptionsTaskTaskTest {

    ThreadPoolService threadPoolService;

    RepublishPartialSubscriptionsTask republishPartialSubscriptionsTask;

    @BeforeEach
    void prepare() {
        threadPoolService = MockGenerator.mockThreadPoolService();

        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "timestamp"));
        var fakeDocs = MockGenerator.createFakeMessageStateMongoDocuments(10, ENV, Status.WAITING,false);
        var fakeMessageStates = new SliceImpl<>(fakeDocs, pageable, true);

        // findByStatusWaitingOrFailedWithCallbackExceptionAndSubscriptionIdsAndTimestampLessThanEqual ( SSE )
        when(MockGenerator.messageStateMongoRepo.findByStatusWaitingOrFailedWithCallbackExceptionAndSubscriptionIdsAndTimestampLessThanEqual(anyList(), eq(List.of(SUBSCRIPTION_ID)), any(), any()))
                .thenReturn( fakeMessageStates ).thenReturn( new SliceImpl<>(new ArrayList<>()) );

        // findByStatusInAndDeliveryTypeAndSubscriptionIds ( CALLBACK )
        when(MockGenerator.messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdsAsc(anyList(), any(DeliveryType.class), anyList(), any()))
                .thenReturn(fakeMessageStates).thenReturn( new SliceImpl<>(new ArrayList<>()) );
    }

    private void assertClearedHealthCheck(HealthCheckData healthCheckData) {
        Assertions.assertEquals(0, healthCheckData.getSubscriptionIds().size());
        Assertions.assertEquals(1, healthCheckData.getRepublishCount());
        Assertions.assertFalse(healthCheckData.getIsThreadOpen().get());
    }

    private void assertNotClearedHealthCheck(HealthCheckData healthCheckData, String subscriptionId) {
        Assertions.assertEquals(1, healthCheckData.getSubscriptionIds().size());
        Assertions.assertEquals(Set.of(subscriptionId), healthCheckData.getSubscriptionIds());
        Assertions.assertEquals(0, healthCheckData.getRepublishCount());
        Assertions.assertTrue(healthCheckData.getIsThreadOpen().get());
    }



    @Test
    void should_work_when_entry_in_HealthCache_for_all_subscriptionIds() throws JsonProcessingException {
        var fakeCircuitBreakerMessage =  MockGenerator.createFakeCircuitBreakerMessages(1).get(0);
        var fakeCircuitBreakerMessageRepublishing = SerializationUtils.clone(fakeCircuitBreakerMessage);
        fakeCircuitBreakerMessageRepublishing.setStatus(CircuitBreakerStatus.REPUBLISHING);
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessage( eq(SUBSCRIPTION_ID) ))
                .thenReturn(Optional.ofNullable(fakeCircuitBreakerMessage))
                .thenReturn(Optional.of(fakeCircuitBreakerMessageRepublishing));


        var pa = PartialSubscription.fromCircuitBreakerMessage(fakeCircuitBreakerMessage);


        when(MockGenerator.kafkaTemplate.receive(any(), anyInt(), anyLong(), any(Duration.class))).thenReturn(MockGenerator.createFakeConsumerRecord( DeliveryType.CALLBACK ));
        MockGenerator.healthCheckCache.add( CALLBACK_URL, HttpMethod.HEAD, SUBSCRIPTION_ID );

        republishPartialSubscriptionsTask = new RepublishPartialSubscriptionsTask(List.of(pa), threadPoolService);
        republishPartialSubscriptionsTask.run();

        verify(MockGenerator.circuitBreakerCache, times(1)).updateCircuitBreakerStatus(eq(SUBSCRIPTION_ID), eq(CircuitBreakerStatus.REPUBLISHING));
        verify(MockGenerator.circuitBreakerCache, times(1)).closeCircuitBreakersIfRepublishing(eq(List.of(SUBSCRIPTION_ID)));

        var healthChecks = MockGenerator.healthCheckCache.getAll();
        Assertions.assertEquals(1, healthChecks.size());
        var firstHealthCheck = healthChecks.iterator().next();
        // All subscriptionIds should be removed
        assertClearedHealthCheck(firstHealthCheck);

        log.info("healthChecks: {}", healthChecks);
    }


    @Test
    void should_work_when_entry_in_HealthCache_for_partial_subscriptionIds() throws JsonProcessingException {
        var fakeCircuitBreakerMessage1 =  MockGenerator.createFakeCircuitBreakerMessages(1, false, false).get(0);
        var fakeCircuitBreakerMessage2 =  MockGenerator.createFakeCircuitBreakerMessages(1, true, false).get(0); // same callbackUrl to have the same cache entry

        var fakeCircuitBreakerMessageRepublishing = SerializationUtils.clone(fakeCircuitBreakerMessage1);
        fakeCircuitBreakerMessageRepublishing.setStatus(CircuitBreakerStatus.REPUBLISHING);
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessage( eq(SUBSCRIPTION_ID) ))
                .thenReturn(Optional.ofNullable(fakeCircuitBreakerMessage1))
                .thenReturn(Optional.of(fakeCircuitBreakerMessageRepublishing));

        // partial subscription for fakeCircuitBreakerMessage1
        var partialSubscription1 = PartialSubscription.fromCircuitBreakerMessage(fakeCircuitBreakerMessage1);
        var partialSubscription2 = PartialSubscription.fromCircuitBreakerMessage(fakeCircuitBreakerMessage2);

        when(MockGenerator.kafkaTemplate.receive(any(), anyInt(), anyLong(), any(Duration.class))).thenReturn(MockGenerator.createFakeConsumerRecord( DeliveryType.CALLBACK ));
        MockGenerator.healthCheckCache.add( CALLBACK_URL, HttpMethod.HEAD,SUBSCRIPTION_ID ); // for partialSubscription1
        MockGenerator.healthCheckCache.add( CALLBACK_URL, HttpMethod.HEAD, partialSubscription2.subscriptionId() );

        republishPartialSubscriptionsTask = new RepublishPartialSubscriptionsTask(List.of(partialSubscription1), threadPoolService);
        republishPartialSubscriptionsTask.run();

        var healthChecks = MockGenerator.healthCheckCache.getAll();
        log.info("healthChecks: {}", healthChecks);


        verify(MockGenerator.healthCheckCache, times(1)).clearBeforeRepublishing(eq(CALLBACK_URL), eq(HttpMethod.HEAD), eq(List.of(SUBSCRIPTION_ID)));
        verify(MockGenerator.circuitBreakerCache, times(1)).updateCircuitBreakerStatus(eq(SUBSCRIPTION_ID), eq(CircuitBreakerStatus.REPUBLISHING));
        verify(MockGenerator.circuitBreakerCache, times(1)).closeCircuitBreakersIfRepublishing(eq(List.of(SUBSCRIPTION_ID)));

        Assertions.assertEquals(1, healthChecks.size());
        var firstHealthCheck = healthChecks.iterator().next();

        assertNotClearedHealthCheck(firstHealthCheck, partialSubscription2.subscriptionId());
    }


    @Test
    void should_work_when_entry_in_HealthCache_for_multiple_entries() throws JsonProcessingException {
        var fakeCircuitBreakerMessage1 =  MockGenerator.createFakeCircuitBreakerMessages(1, false, false).get(0);
        var fakeCircuitBreakerMessage2 =  MockGenerator.createFakeCircuitBreakerMessages(1, true, true).get(0); // different callbackUrl to have the different cache entries

        var fakeCircuitBreakerMessageRepublishing = SerializationUtils.clone(fakeCircuitBreakerMessage1);
        fakeCircuitBreakerMessageRepublishing.setStatus(CircuitBreakerStatus.REPUBLISHING);
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessage( eq(SUBSCRIPTION_ID) ))
                .thenReturn(Optional.ofNullable(fakeCircuitBreakerMessage1))
                .thenReturn(Optional.of(fakeCircuitBreakerMessageRepublishing));

        // partial subscription for fakeCircuitBreakerMessage1
        var partialSubscription1 = PartialSubscription.fromCircuitBreakerMessage(fakeCircuitBreakerMessage1);
        var partialSubscription2 = PartialSubscription.fromCircuitBreakerMessage(fakeCircuitBreakerMessage2);

        when(MockGenerator.kafkaTemplate.receive(any(), anyInt(), anyLong(), any(Duration.class))).thenReturn(MockGenerator.createFakeConsumerRecord( DeliveryType.CALLBACK ));
        MockGenerator.healthCheckCache.add( CALLBACK_URL, HttpMethod.HEAD,SUBSCRIPTION_ID ); // for partialSubscription1
        MockGenerator.healthCheckCache.add( fakeCircuitBreakerMessage2.getCallbackUrl(), HttpMethod.HEAD, partialSubscription2.subscriptionId() );

        republishPartialSubscriptionsTask = new RepublishPartialSubscriptionsTask(List.of(partialSubscription1), threadPoolService);
        republishPartialSubscriptionsTask.run();



        verify(MockGenerator.healthCheckCache, times(1)).clearBeforeRepublishing(eq(CALLBACK_URL), eq(HttpMethod.HEAD), eq(List.of(SUBSCRIPTION_ID)));
        verify(MockGenerator.circuitBreakerCache, times(1)).updateCircuitBreakerStatus(eq(SUBSCRIPTION_ID), eq(CircuitBreakerStatus.REPUBLISHING));
        verify(MockGenerator.circuitBreakerCache, times(1)).closeCircuitBreakersIfRepublishing(eq(List.of(SUBSCRIPTION_ID)));

        var healthChecks = MockGenerator.healthCheckCache.getAll();
        log.info("healthChecks: {}", healthChecks);
        var oShouldBeClearedHealthCheck = MockGenerator.healthCheckCache.get(CALLBACK_URL, HttpMethod.HEAD);
        Assertions.assertTrue(oShouldBeClearedHealthCheck.isPresent());
        var shouldBeClearedHealthCheck = oShouldBeClearedHealthCheck.get();

        Assertions.assertEquals(2, healthChecks.size());
        // All subscriptionIds should be removed
        assertClearedHealthCheck(shouldBeClearedHealthCheck);


        var oShouldNotBeClearedHealthCheck = MockGenerator.healthCheckCache.get(fakeCircuitBreakerMessage2.getCallbackUrl(), HttpMethod.HEAD);
        Assertions.assertTrue(oShouldNotBeClearedHealthCheck.isPresent());
        var shouldNotBeClearedHealthCheck = oShouldNotBeClearedHealthCheck.get();
        assertNotClearedHealthCheck(shouldNotBeClearedHealthCheck, partialSubscription2.subscriptionId());
    }

}
