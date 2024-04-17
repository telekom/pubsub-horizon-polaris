// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.mongodb.assertions.Assertions;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.AbstractIntegrationTest;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static de.telekom.horizon.polaris.TestConstants.CALLBACK_URL;
import static de.telekom.horizon.polaris.TestConstants.SUBSCRIPTION_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.timeout;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Slf4j
@AutoConfigureMockMvc(addFilters = false)
class SimpleIntegrationTest extends AbstractIntegrationTest {
    private final static String TRACING_HEADER_NAME = "x-b3-traceid";

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ScheduledEventWaitingHandler scheduledEventWaitingHandler;

    @Autowired
    private CircuitBreakerManager circuitBreakerManager;
    @Autowired
    private HealthCheckCache healthCheckCache;

    @SpyBean
    private CircuitBreakerCacheService circuitBreakerCacheService;
    @SpyBean
    private ThreadPoolService threadPoolService;
    private SubscriptionEventMessage subscriptionMessage_WAITING;
    private PartialSubscription partialSubscription;


    @BeforeEach
    void beforeEach() throws ExecutionException, JsonProcessingException, InterruptedException {
        wireMockServer.stubFor(
                head(new UrlPattern(equalTo(CALLBACK_URL), false)).willReturn(aResponse().withStatus(HttpStatus.OK.value()))
        );

        partialSubscription = addFakePartialSubscription(SUBSCRIPTION_ID, CALLBACK_URL);
        subscriptionMessage_WAITING = generateFakeSubscriptionEventMessage(SUBSCRIPTION_ID, CALLBACK_URL);

    }

    @Test
    void testSimpleRepublishing() throws Exception {
        HttpMethod httpMethod = partialSubscription.isGetMethodInsteadOfHead() ? HttpMethod.GET : HttpMethod.HEAD;
        String fullCallbackUrl = partialSubscription.callbackUrl();

        var sendResult = simulateNewPublishedEvent(subscriptionMessage_WAITING);
        simulateOpenCircuitBreaker(partialSubscription, subscriptionMessage_WAITING.getUuid(), sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());

        assertTrue(healthCheckCache.get(fullCallbackUrl, httpMethod).isEmpty());

        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();

        Mockito.verify(circuitBreakerCacheService, timeout(300000).times(1)).updateCircuitBreakerStatus(SUBSCRIPTION_ID, CircuitBreakerStatus.REPUBLISHING);
        Mockito.verify(circuitBreakerCacheService, timeout(300000).times(1)).closeCircuitBreaker(SUBSCRIPTION_ID);

        wireMockServer.verify(
                exactly(1),
                headRequestedFor(
                        urlPathEqualTo(CALLBACK_URL)
                )
        );



        var recordIn_WAITING = pollForRecord(3, TimeUnit.SECONDS);  // The event we send in, this was getting picked
        var recordOut_PROCESSED = pollForRecord(5, TimeUnit.SECONDS); // The event the polaris republished
        assertNotNull(recordOut_PROCESSED);
        assertFalse(recordOut_PROCESSED.offset() == 0 && recordOut_PROCESSED.partition() == 0);

        SubscriptionEventMessage subscriptionMessage_PROCESSED = objectMapper.readValue(recordOut_PROCESSED.value(), SubscriptionEventMessage.class);
        assertNotNull(subscriptionMessage_PROCESSED);
        assertEquals(Status.PROCESSED, subscriptionMessage_PROCESSED.getStatus());
        assertEquals(recordIn_WAITING.key(), subscriptionMessage_PROCESSED.getUuid());


        // Assert correct health check cache state
        assertFalse(healthCheckCache.get(fullCallbackUrl, httpMethod).isEmpty());
        assertFalse(healthCheckCache.get(fullCallbackUrl, httpMethod).get().isThreadOpen());
        assertTrue(healthCheckCache.get(fullCallbackUrl, httpMethod).get().getSubscriptionIds().isEmpty());
        assertEquals(1, healthCheckCache.get(fullCallbackUrl, httpMethod).get().getRepublishCount());
        assertEquals(200, healthCheckCache.get(fullCallbackUrl, httpMethod).get().getLastHealthCheckOrNull().getReturnCode());
    }


    @Test
    void testSimpleCouldNotPick() throws InterruptedException, JsonProcessingException, ExecutionException {
        var sendResult = simulateNewPublishedEvent(subscriptionMessage_WAITING);
        simulateOpenCircuitBreaker(partialSubscription, subscriptionMessage_WAITING.getUuid(), 0, 9999); // some invalid offset

        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();

        Mockito.verify(circuitBreakerCacheService, timeout(1000000).times(1)).updateCircuitBreakerStatus(SUBSCRIPTION_ID, CircuitBreakerStatus.REPUBLISHING);
        Mockito.verify(circuitBreakerCacheService, timeout(1000000).times(1)).closeCircuitBreaker(SUBSCRIPTION_ID);

        wireMockServer.verify(
                exactly(1),
                headRequestedFor(
                        urlPathEqualTo(CALLBACK_URL)
                )
        );

        var recordIn_WAITING = pollForRecord(3, TimeUnit.SECONDS);  // The event we send in, this was getting picked
        var recordOut_PROCESSED = pollForRecord(5, TimeUnit.SECONDS); // The event the polaris republished
        assertNotNull(recordOut_PROCESSED);
        assertFalse(recordOut_PROCESSED.offset() == 0 && recordOut_PROCESSED.partition() == 0);

        SubscriptionEventMessage subscriptionMessage_PROCESSED = objectMapper.readValue(recordOut_PROCESSED.value(), SubscriptionEventMessage.class);
        assertNotNull(subscriptionMessage_PROCESSED);
        assertEquals(Status.FAILED, subscriptionMessage_PROCESSED.getStatus());
        assertEquals(recordIn_WAITING.key(), subscriptionMessage_PROCESSED.getUuid());
    }

    @Test
    void simpleTestRebalanceFromRemovedPod() throws InterruptedException, JsonProcessingException, ExecutionException {
        wireMockServer.resetAll();
        wireMockServer.stubFor(
                head(new UrlPattern(equalTo(CALLBACK_URL), false)).willReturn(aResponse().withStatus(HttpStatus.FORBIDDEN.value()))
        );

        final String anotherPodName = "SomePodName";
        polarisPodCache.add(anotherPodName);

        var sendResult = simulateNewPublishedEvent(subscriptionMessage_WAITING);
        simulateCircuitBreaker(partialSubscription, subscriptionMessage_WAITING.getUuid(), sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset(), anotherPodName, CircuitBreakerStatus.OPEN);

        // Same 2 liner is used in the PdoResourceEventHandle#onDelete
        polarisPodCache.remove(anotherPodName);
        circuitBreakerManager.rebalanceFromRemovedPod(anotherPodName);

        // All circuit breaker messages should be assigned to this pod
        var cbs = circuitBreakerCacheService.getCircuitBreakerMessages(0, 10000, polarisConfig.getPodName());
        assertEquals( 1, cbs.size());

        Mockito.verify(threadPoolService, timeout(1000000).times(1)).startHealthRequestTask( eq(wireMockServer.baseUrl() + CALLBACK_URL), anyString(), anyString(), anyString(), any(), eq(HttpMethod.HEAD), any());
        Mockito.verify(circuitBreakerCacheService, timeout(1000000).times(1)).updateCircuitBreakerMessage(argThat(a -> Objects.equals(a.getSubscriptionId(), SUBSCRIPTION_ID) && a.getLastHealthCheck() != null));
        wireMockServer.verify(1, headRequestedFor(urlPathEqualTo(CALLBACK_URL)));
    }

    private PartialSubscription addFakePartialSubscription(String subscriptionId, String callbackUrl) {
        SubscriptionResource subscriptionResource = MockGenerator.createFakeSubscriptionResource("playground", getEventType());
        subscriptionResource.getSpec().getSubscription().setSubscriptionId(subscriptionId);
        subscriptionResource.getSpec().getSubscription().setCallback(wireMockServer.baseUrl() + callbackUrl);
        return addTestSubscription(subscriptionResource);
    }

    private SubscriptionEventMessage generateFakeSubscriptionEventMessage(String subscriptionId, String callbackUrl) {
        var subscriptionMessage_WAITING = MockGenerator.createFakeSubscriptionEventMessage(subscriptionId, getEventType());
        subscriptionMessage_WAITING.setStatus(Status.WAITING);
        subscriptionMessage_WAITING.setAdditionalFields(new HashMap<>());

        return subscriptionMessage_WAITING;
    }

}
