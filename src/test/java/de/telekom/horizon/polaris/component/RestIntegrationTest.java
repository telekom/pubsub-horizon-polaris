package de.telekom.horizon.polaris.component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.component.rest.HealthCheckController;
import de.telekom.horizon.polaris.model.CloseCircuitBreakersRequest;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.AbstractIntegrationTest;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.*;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static de.telekom.horizon.polaris.TestConstants.CALLBACK_URL;
import static de.telekom.horizon.polaris.TestConstants.SUBSCRIPTION_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Slf4j
@AutoConfigureMockMvc(addFilters = false)
public class RestIntegrationTest extends AbstractIntegrationTest {

    @SpyBean
    private HealthCheckController healthCheckController;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ScheduledEventWaitingHandler scheduledEventWaitingHandler;
    @SpyBean
    private ThreadPoolService threadPoolService;

    private SubscriptionEventMessage subscriptionMessage_WAITING;
    private SubscriptionEventMessage subscriptionMessage_WAITING2;
    private PartialSubscription partialSubscription_HEAD;

    @SpyBean
    private CircuitBreakerCacheService circuitBreakerCacheService;


    @BeforeEach
    void beforeEach() throws ExecutionException, JsonProcessingException, InterruptedException {
        wireMockServer.stubFor(
                head(new UrlPattern(equalTo(CALLBACK_URL), false))
                        .willReturn(aResponse().withStatus(HttpStatus.SERVICE_UNAVAILABLE.value()))
        );

        partialSubscription_HEAD = addFakePartialSubscription(SUBSCRIPTION_ID, CALLBACK_URL, false);
        subscriptionMessage_WAITING = generateFakeSubscriptionEventMessage(SUBSCRIPTION_ID, CALLBACK_URL);

        // Clean open threads, health cache and circuit breaker cache
        threadPoolService.getRequestingTasks().forEach((callbackKey, listenableScheduledFuture) -> threadPoolService.stopHealthRequestTask(callbackKey.callbackUrl(), callbackKey.httpMethod()));
        healthCheckCache.getAllKeys().asIterator().forEachRemaining(c -> healthCheckCache.remove(c.callbackUrl(), c.httpMethod()));
        circuitBreakerCacheService.getCircuitBreakerMessages().forEach(circuitBreakerMessage -> circuitBreakerCacheService.closeCircuitBreaker(circuitBreakerMessage.getSubscriptionId()));
    }

    @Test
    void testGetForNonExistentCallbackUrl() throws Exception {
        final String fakeCallbackUrl = "https://fake.url";

        mockMvc.perform(get("/health-checks?callbackUrl=" + fakeCallbackUrl))
                .andDo(print())
                .andExpect(status().isNotFound())
                .andReturn();
    }

    @Test
    void testGetForCallbackUrl() throws Exception {
        // Another Subscription for another http method (GET)
        final String SUBSCRIPTION_ID2 = SUBSCRIPTION_ID + "2";
        var partialSubscription_GET = addFakePartialSubscription(SUBSCRIPTION_ID2, CALLBACK_URL, true);
        var subscriptionMessage_WAITING2 = generateFakeSubscriptionEventMessage(SUBSCRIPTION_ID2, CALLBACK_URL);

        // Another Subscription for already HEAD http method
        final String SUBSCRIPTION_ID3 = SUBSCRIPTION_ID + "3";
        var partialSubscription_HEAD2 = addFakePartialSubscription(SUBSCRIPTION_ID3, CALLBACK_URL, false);
        var subscriptionMessage_WAITING3 = generateFakeSubscriptionEventMessage(SUBSCRIPTION_ID3, CALLBACK_URL);


        var sendResult = simulateNewPublishedEvent(subscriptionMessage_WAITING);
        var sendResult2 = simulateNewPublishedEvent(subscriptionMessage_WAITING2);
        var sendResult3 = simulateNewPublishedEvent(subscriptionMessage_WAITING3);
        simulateOpenCircuitBreaker(partialSubscription_HEAD, subscriptionMessage_WAITING.getUuid(), sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());;
        simulateOpenCircuitBreaker(partialSubscription_GET, subscriptionMessage_WAITING2.getUuid(), sendResult2.getRecordMetadata().partition(), sendResult2.getRecordMetadata().offset());;
        simulateOpenCircuitBreaker(partialSubscription_HEAD2, subscriptionMessage_WAITING3.getUuid(), sendResult3.getRecordMetadata().partition(), sendResult3.getRecordMetadata().offset());;

        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();

        mockMvc.perform(get("/health-checks?callbackUrl=" + partialSubscription_HEAD.callbackUrl()))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].subscriptionIds", Matchers.containsInAnyOrder(SUBSCRIPTION_ID, SUBSCRIPTION_ID3)))
                .andExpect(MockMvcResultMatchers.jsonPath("$[1].subscriptionIds[0]").value(SUBSCRIPTION_ID2))
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].isThreadOpen").value(true))
                .andExpect(MockMvcResultMatchers.jsonPath("$[1].isThreadOpen").value(true))
                .andReturn();

        mockMvc.perform(get("/circuit-breakers?"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$[*].subscriptionId", Matchers.containsInAnyOrder(SUBSCRIPTION_ID,SUBSCRIPTION_ID2, SUBSCRIPTION_ID3)))
                .andExpect(MockMvcResultMatchers.jsonPath("$[*].status", Matchers.everyItem(Matchers.equalTo(CircuitBreakerStatus.CHECKING.toString()))))
                .andReturn();
    }

    @Test
    void testGetForCallbackUrlAndHttpMethodAfterSuccessfulRedelivery() throws Exception {
        wireMockServer.resetAll();
        wireMockServer.stubFor(
                head(new UrlPattern(equalTo(CALLBACK_URL), false))
                        .willReturn(aResponse().withStatus(HttpStatus.OK.value()))
        );

        var sendResult = simulateNewPublishedEvent(subscriptionMessage_WAITING);
        simulateOpenCircuitBreaker(partialSubscription_HEAD, subscriptionMessage_WAITING.getUuid(), sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());;

        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();

        Mockito.verify(circuitBreakerCacheService, timeout(30000).times(1)).updateCircuitBreakerStatus(SUBSCRIPTION_ID, CircuitBreakerStatus.REPUBLISHING);
        Mockito.verify(circuitBreakerCacheService, timeout(30000).times(1)).closeCircuitBreaker(SUBSCRIPTION_ID);

        wireMockServer.verify(
                exactly(1),
                headRequestedFor(
                        urlPathEqualTo(CALLBACK_URL)
                )
        );

        HttpMethod httpMethod = partialSubscription_HEAD.isGetMethodInsteadOfHead() ? HttpMethod.GET : HttpMethod.HEAD;

        mockMvc.perform(get("/health-checks?callbackUrl=" + partialSubscription_HEAD.callbackUrl() + "&httpMethod=" + httpMethod))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.subscriptionIds[0]").doesNotExist())
                .andExpect(MockMvcResultMatchers.jsonPath("$.isThreadOpen").value(false))
                .andReturn();

        mockMvc.perform(get("/circuit-breakers/" + partialSubscription_HEAD.subscriptionId()))
                .andDo(print())
                .andExpect(status().isNotFound())
                .andReturn();

    }

    @Test
    void testGetForCallbackUrlAndHttpMethodAfterUnsuccessfulRedelivery() throws Exception {
        // Mock Service Unavailable


        var sendResult = simulateNewPublishedEvent(subscriptionMessage_WAITING);
        simulateOpenCircuitBreaker(partialSubscription_HEAD, subscriptionMessage_WAITING.getUuid(), sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());;

        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();

        // When CB gets set to CHECKING
        Mockito.verify(circuitBreakerCacheService, timeout(30000).times(1))
                .updateCircuitBreakerMessage(
                        argThat(am -> am.getSubscriptionId().equals(SUBSCRIPTION_ID) && am.getStatus().equals(CircuitBreakerStatus.CHECKING)));

        // After the Health request was made to the customers callback url
        Mockito.verify(circuitBreakerCacheService, timeout(30000).times(1))
                .updateCircuitBreakerMessage(
                        argThat(am -> am.getSubscriptionId().equals(SUBSCRIPTION_ID) && am.getLastHealthCheck() != null));

        wireMockServer.verify(
                exactly(1),
                headRequestedFor(
                        urlPathEqualTo(CALLBACK_URL)
                )
        );

        HttpMethod httpMethod = partialSubscription_HEAD.isGetMethodInsteadOfHead() ? HttpMethod.GET : HttpMethod.HEAD;

        mockMvc.perform(get(MessageFormat.format("/health-checks?callbackUrl={0}&httpMethod={1}", partialSubscription_HEAD.callbackUrl(), httpMethod)))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.subscriptionIds[0]").value(SUBSCRIPTION_ID))
                .andExpect(MockMvcResultMatchers.jsonPath("$.isThreadOpen").value(true))
                .andExpect(MockMvcResultMatchers.jsonPath("$.lastHealthCheckOrNull.returnCode").value(503))
                .andReturn();


        mockMvc.perform(get("/circuit-breakers/" + partialSubscription_HEAD.subscriptionId()))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.subscriptionId").value(SUBSCRIPTION_ID))
                .andExpect(MockMvcResultMatchers.jsonPath("$.status").value(CircuitBreakerStatus.CHECKING.toString()))
                .andReturn();
    }


    @Test
    void testClosingOpenCircuitBreaker() throws Exception {
        var sendResult = simulateNewPublishedEvent(subscriptionMessage_WAITING);
        simulateOpenCircuitBreaker(partialSubscription_HEAD, subscriptionMessage_WAITING.getUuid(), sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());;

        HttpMethod httpMethod = partialSubscription_HEAD.isGetMethodInsteadOfHead() ? HttpMethod.GET : HttpMethod.HEAD;

        var closeCircuitBreakersRequest = new CloseCircuitBreakersRequest(List.of(partialSubscription_HEAD.subscriptionId()));
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(closeCircuitBreakersRequest);

        mockMvc.perform(
                    MockMvcRequestBuilders.delete("/circuit-breakers")
                            .contentType(MediaType.APPLICATION_JSON)
                            .characterEncoding("utf-8")
                            .content(json)
                )
                .andDo(print())
                .andExpect(status().isTooEarly())
                .andReturn();
    }

    @Test
    void testClosingRepublishingCircuitBreaker() throws Exception {
        var sendResult = simulateNewPublishedEvent(subscriptionMessage_WAITING);
        simulateRepublishingCircuitBreaker(partialSubscription_HEAD, subscriptionMessage_WAITING.getUuid(), sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());;

        var closeCircuitBreakersRequest = new CloseCircuitBreakersRequest(List.of(partialSubscription_HEAD.subscriptionId()));
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(closeCircuitBreakersRequest);

        mockMvc.perform(
                        MockMvcRequestBuilders.delete("/circuit-breakers")
                                .contentType(MediaType.APPLICATION_JSON)
                                .characterEncoding("utf-8")
                                .content(json)
                )
                .andDo(print())
                .andExpect(status().isConflict())
                .andReturn();
    }

    @Test
    void testClosingCircuitBreaker() throws Exception {
        var sendResult = simulateNewPublishedEvent(subscriptionMessage_WAITING);
        simulateOpenCircuitBreaker(partialSubscription_HEAD, subscriptionMessage_WAITING.getUuid(), sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());;

        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();

        // Wait for first health check
        // After the Health request was made to the customers callback url
        Mockito.verify(circuitBreakerCacheService, timeout(30000).times(1))
                .updateCircuitBreakerMessage(
                        argThat(am -> am.getSubscriptionId().equals(SUBSCRIPTION_ID) && am.getLastHealthCheck() != null));


        var closeCircuitBreakersRequest = new CloseCircuitBreakersRequest(List.of(partialSubscription_HEAD.subscriptionId()));
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(closeCircuitBreakersRequest);

        mockMvc.perform(
                        MockMvcRequestBuilders.delete("/circuit-breakers")
                                .contentType(MediaType.APPLICATION_JSON)
                                .characterEncoding("utf-8")
                                .content(json)
                )
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subscriberIdsNotFoundInSubscriptionCache", Matchers.empty()))
                .andReturn();

        // should have republished even thought endpoint was not reachable
        // also there should be no thread handling this task / doing health checks anymore
        assertRepublishedMessageInKafka(objectMapper);

        // No more open thread
        var tasks = threadPoolService.getRequestingTasks();
        if(!tasks.isEmpty()) {
            tasks.values().forEach(task -> assertTrue(task.isCancelled()));
        }

        // No more existing circuit breaker
        var cbs = circuitBreakerCacheService.getCircuitBreakerMessages();
        assertEquals(0, cbs.size());
    }


    @Test
    void testClosingCircuitBreakerWithoutSubscription() throws Exception {
        var sendResult = simulateNewPublishedEvent(subscriptionMessage_WAITING);
        simulateOpenCircuitBreaker(partialSubscription_HEAD, subscriptionMessage_WAITING.getUuid(), sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());;

        scheduledEventWaitingHandler.loadAndProcessOpenCircuitBreakerMessagesScheduled();

        // Wait for first health check
        // After the Health request was made to the customers callback url
        Mockito.verify(circuitBreakerCacheService, timeout(30000).times(1))
                .updateCircuitBreakerMessage(
                        argThat(am -> am.getSubscriptionId().equals(SUBSCRIPTION_ID) && am.getLastHealthCheck() != null));


        // Remove subscription from cache
        partialSubscriptionCache.remove(partialSubscription_HEAD);

        var closeCircuitBreakersRequest = new CloseCircuitBreakersRequest(List.of(partialSubscription_HEAD.subscriptionId()));
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(closeCircuitBreakersRequest);

        mockMvc.perform(
                        MockMvcRequestBuilders.delete("/circuit-breakers")
                                .contentType(MediaType.APPLICATION_JSON)
                                .characterEncoding("utf-8")
                                .content(json)
                )
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subscriberIdsNotFoundInSubscriptionCache", Matchers.contains(SUBSCRIPTION_ID)))
                .andReturn();

        // should have republished even thought endpoint was not reachable
        // also there should be no thread handling this task / doing health checks anymore
        assertRepublishedMessageInKafka(objectMapper);

        // No more open thread
        var tasks = threadPoolService.getRequestingTasks();
        if(!tasks.isEmpty()) {
            tasks.values().forEach(task -> assertTrue(task.isCancelled()));
        }

        // No more existing circuit breaker
        var cbs = circuitBreakerCacheService.getCircuitBreakerMessages();
        assertEquals(0, cbs.size());
    }

    private void assertRepublishedMessageInKafka(ObjectMapper objectMapper) throws InterruptedException, JsonProcessingException {
        // Check on republished event
        var recordIn_WAITING = pollForRecord(3, TimeUnit.SECONDS);  // The event we send in, this was getting picked
        var recordOut_PROCESSED = pollForRecord(5, TimeUnit.SECONDS); // The event the polaris republished
        assertNotNull(recordOut_PROCESSED);
        assertFalse(recordOut_PROCESSED.offset() == 0 && recordOut_PROCESSED.partition() == 0);
        SubscriptionEventMessage subscriptionMessage_PROCESSED = objectMapper.readValue(recordOut_PROCESSED.value(), SubscriptionEventMessage.class);
        assertNotNull(subscriptionMessage_PROCESSED);
        assertEquals(Status.PROCESSED, subscriptionMessage_PROCESSED.getStatus()); // status PROCESSED
        assertEquals(recordIn_WAITING.key(), subscriptionMessage_PROCESSED.getUuid()); // same UUID

    }

    @Test
    void testClosingNonExistingCircuitBreaker() throws Exception {
        var closeCircuitBreakersRequest = new CloseCircuitBreakersRequest(List.of(partialSubscription_HEAD.subscriptionId()));
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(closeCircuitBreakersRequest);

        mockMvc.perform(
                        MockMvcRequestBuilders.delete("/circuit-breakers")
                                .contentType(MediaType.APPLICATION_JSON)
                                .characterEncoding("utf-8")
                                .content(json)
                )
                .andDo(print())
                .andExpect(status().isNotFound())
                .andReturn();
    }


    private PartialSubscription addFakePartialSubscription(String subscriptionId, String callbackUrl, boolean isGetInsteadOfHead) {
        SubscriptionResource subscriptionResource = MockGenerator.createFakeSubscriptionResource("playground", getEventType());
        subscriptionResource.getSpec().getSubscription().setSubscriptionId(subscriptionId);
        subscriptionResource.getSpec().getSubscription().setEnforceGetHttpRequestMethodForHealthCheck(isGetInsteadOfHead);
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
