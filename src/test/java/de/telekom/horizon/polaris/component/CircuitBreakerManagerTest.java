// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;


import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.exception.CouldNotDetermineWorkingSetException;
import de.telekom.horizon.polaris.kubernetes.PodResourceEventHandler;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static de.telekom.horizon.polaris.TestConstants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Slf4j
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CircuitBreakerManagerTest {
    @Mock
    ThreadPoolService threadPoolService;
    CircuitBreakerManager circuitBreakerManager;

    PodResourceEventHandler podResourceEventHandler;

    @BeforeEach
    void prepare() {
        threadPoolService = MockGenerator.mockThreadPoolService();

        when(MockGenerator.partialSubscriptionCache.get(anyString())).then( a -> {
            var fakePartialSubscription = new PartialSubscription(ENV, a.getArgument(0, String.class), PUBLISHER_ID, SUBSCRIBER_ID, CALLBACK_URL, DeliveryType.CALLBACK, false, false);
            return Optional.ofNullable(fakePartialSubscription);
        });

        when(MockGenerator.polarisConfig.getPollingBatchSize()).thenReturn(100);

        circuitBreakerManager = spy(new CircuitBreakerManager(threadPoolService));
        podResourceEventHandler = new PodResourceEventHandler(MockGenerator.polarisPodCache, circuitBreakerManager);

    }

    @ParameterizedTest
    @CsvSource( value = {
            "2, 1000",
            "4, 1000",
            "8, 10000",
            "16, 20000",
    })
    void should_rebalance(int nrOfPods, int nrOfCircuitBreakerMessages) throws CouldNotDetermineWorkingSetException {
        var fakePods = initPods(nrOfPods);

        var fakePodNames = fakePods.stream().map(fp -> fp.getMetadata().getName()).toList();

        var fakeCircuitBreakerMessages = MockGenerator.createFakeCircuitBreakerMessages(nrOfCircuitBreakerMessages, true, true);
        for(int i = 0; i < fakeCircuitBreakerMessages.size(); i++) {
            var fakeCircuitBreakerMessage = fakeCircuitBreakerMessages.get(i);

            var podIndexToAssign = i % fakePods.size();

            fakeCircuitBreakerMessage.setStatus(CircuitBreakerStatus.CHECKING);
            fakeCircuitBreakerMessage.setAssignedPodId(fakePods.get(podIndexToAssign).getMetadata().getName());
        }

        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(0), anyInt()))
                .thenReturn(fakeCircuitBreakerMessages);
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessages(eq(1), anyInt()))
                .thenReturn(new ArrayList<>());

        // Now circuit breaker messages are evenly spread between pods

        var lastFakePod = fakePods.get(fakePods.size() - 1);
        podResourceEventHandler.onDelete(lastFakePod, true); // should trigger rebalancing

        int oldNrOfCbMsgsPerPod = (int) Math.ceil(fakeCircuitBreakerMessages.size() / fakePods.size()); // before the rebalancing -> this number should be rebalanced onto x new pods
        int newNrOfCbMsgsPerPod = (int) Math.ceil(fakeCircuitBreakerMessages.size() / (fakePods.size() - 1)); // after rebalancing -> this number should have very pod now
        var nrOfNewMsgsPerPod = newNrOfCbMsgsPerPod - oldNrOfCbMsgsPerPod;

        log.info("oldNrOfCbMsgsPerPod: {}", oldNrOfCbMsgsPerPod);
        log.info("newNrOfCbMsgsPerPod: {}", newNrOfCbMsgsPerPod);
        log.info("nrOfNewMsgsPerPod: {}", nrOfNewMsgsPerPod);


        // We can not calc an excact number, because of the random callback ur and the random hash
        verify(MockGenerator.circuitBreakerCache, atLeast(nrOfNewMsgsPerPod / 2)).updateCircuitBreakerMessage( argThat( circuitBreakerMessage -> circuitBreakerMessage.getAssignedPodId().equals(POD_NAME)) );
        verify(MockGenerator.circuitBreakerCache, atMost(oldNrOfCbMsgsPerPod)).updateCircuitBreakerMessage( argThat( circuitBreakerMessage -> circuitBreakerMessage.getAssignedPodId().equals(POD_NAME)) );

    }


    // We had a bug, where when the callback url was changed,
    // the first check in claimCircuitBreakerMessageIfPossible assigned one pod (based of OLD callbackUrl (from circuit-breaker-message),
    // but in the SubscriptionComparisonTask the shouldCallbackUrlBeHandledByThisPod used the NEW callbackUrl (from Subscription)
    // therefore the SubscriptionComparisonTask stopped handling the circuit breaker message, bc it thought another pod should handle it.
    @Test
    void should_claimCircuitBreakerMessageIfPossible_when_callbackUrl_changed() throws CouldNotDetermineWorkingSetException, ExecutionException, InterruptedException {
        initPods(2);

        final var firstFakeCircuitBreakerMessage = MockGenerator.createFakeCircuitBreakerMessages(1, true, false).get(0);
        firstFakeCircuitBreakerMessage.setStatus(CircuitBreakerStatus.CHECKING);
        firstFakeCircuitBreakerMessage.setAssignedPodId("SOME_NON_EXISTING_POD");

        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessage(anyString())).thenReturn(Optional.of(firstFakeCircuitBreakerMessage));

        // Return Subscription with NEW callbackUrl
        when(MockGenerator.partialSubscriptionCache.get(anyString())).then( a -> {
            var fakePartialSubscription = new PartialSubscription(ENV, a.getArgument(0, String.class), PUBLISHER_ID, SUBSCRIBER_ID, CALLBACK_URL_NEW, DeliveryType.CALLBACK, false, false);
            return Optional.ofNullable(fakePartialSubscription);
        });

        boolean wasClaimed = circuitBreakerManager.claimCircuitBreakerMessageIfPossible(firstFakeCircuitBreakerMessage);
        Assertions.assertTrue(wasClaimed); // should return true, because CALLBACK_URL % 2 is 0 => our pod (POD_NAME)

        try {
            verify(threadPoolService, timeout(5000)).startSubscriptionComparisonTask(
                    argThat(v -> CALLBACK_URL.equals(v.callbackUrl())),
                    argThat(v -> CALLBACK_URL_NEW.equals(v.callbackUrl())));
            verify(threadPoolService, timeout(5000)).startHealthRequestTask( eq(CALLBACK_URL_NEW) , eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD), any());
            Assertions.assertTrue(true);
        } catch (Exception ex) {
            Assertions.fail(ex.getMessage());
        }

    }

    @Test
    void should_claimCircuitBreakerMessageIfPossible_when_callbackUrl_is_same() throws CouldNotDetermineWorkingSetException {
        initPods(2);

        final var firstFakeCircuitBreakerMessage = MockGenerator.createFakeCircuitBreakerMessages(1, true, false).get(0);
        firstFakeCircuitBreakerMessage.setStatus(CircuitBreakerStatus.CHECKING);
        firstFakeCircuitBreakerMessage.setAssignedPodId("SOME_NON_EXISTING_POD");

        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessage(anyString())).thenReturn(Optional.of(firstFakeCircuitBreakerMessage));


        boolean wasClaimed = circuitBreakerManager.claimCircuitBreakerMessageIfPossible(firstFakeCircuitBreakerMessage);
        Assertions.assertTrue(wasClaimed); // should return true, because CALLBACK_URL % 2 is 0 => our pod (POD_NAME)

        try {
            verify(threadPoolService, timeout(5000)).startSubscriptionComparisonTask(
                    argThat(v -> CALLBACK_URL.equals(v.callbackUrl())),
                    argThat(v -> CALLBACK_URL.equals(v.callbackUrl())));
            verify(threadPoolService, timeout(5000)).startHealthRequestTask( eq(CALLBACK_URL) , eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV), eq(HttpMethod.HEAD), any());
            Assertions.assertTrue(true);
        } catch (Exception ex) {
            Assertions.fail(ex.getMessage());
        }
    }

    @Test
    void should_start_HealthRequestTask_with_delay_on_loop() throws CouldNotDetermineWorkingSetException {
        var fakePods = initPods(1);
        var fakePod = fakePods.get(0);

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

    private List<Pod> initPods(int nrOfPods) {
        var fakePods = MockGenerator.createFakePods(nrOfPods);
        // Set first pod name to static pod name to verify the updates
        var firstFakePod = fakePods.get(0);
        firstFakePod.getMetadata().setName(POD_NAME);

        for(var fakePod : fakePods) {
            podResourceEventHandler.onAdd(fakePod);
        }
        return fakePods;
    }
}
