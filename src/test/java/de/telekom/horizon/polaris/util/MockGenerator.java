// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.util;

import brave.ScopedSpan;
import brave.Span;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.kubernetes.resource.Subscription;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResourceSpec;
import de.telekom.eni.pandora.horizon.model.db.Coordinates;
import de.telekom.eni.pandora.horizon.model.db.State;
import de.telekom.eni.pandora.horizon.model.db.StateProperty;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.component.CircuitBreakerManager;
import de.telekom.horizon.polaris.component.HealthCheckRestClient;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.SubscriptionRepublishingHolder;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.service.WorkerService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.http.StatusLine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;

import java.sql.Date;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static de.telekom.horizon.polaris.TestConstants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MockGenerator {
    public static KafkaTemplate kafkaTemplate;
    public static PartialSubscriptionCache partialSubscriptionCache;
    public static PolarisConfig polarisConfig;
    public static MessageStateMongoRepo messageStateMongoRepo;
    public static HorizonTracer tracer;
    public static ThreadPoolService threadPoolService;
    public static CircuitBreakerCacheService circuitBreakerCache;
    public static HealthCheckCache healthCheckCache;
    public static Environment environment;
    public static CircuitBreakerManager circuitBreakerManager;
    public static HealthCheckRestClient healthCheckRestClient;
    public static EventWriter eventWriter;

    public static MeterRegistry meterRegistry;

    public static SubscriptionRepublishingHolder subscriptionRepublishingHolder;

    public static WorkerService workerService;

    @SneakyThrows
    public static ThreadPoolService mockThreadPoolService() {
        kafkaTemplate = mock(KafkaTemplate.class);

        partialSubscriptionCache = mock(PartialSubscriptionCache.class);
        polarisConfig = mock(PolarisConfig.class);
        messageStateMongoRepo = mock(MessageStateMongoRepo.class);
        tracer = mock(HorizonTracer.class);
        circuitBreakerCache = mock(CircuitBreakerCacheService.class);
        healthCheckCache = spy(new HealthCheckCache());
        threadPoolService = mock(ThreadPoolService.class);
        environment = mock(Environment.class);
        eventWriter = mock(EventWriter.class);
        meterRegistry = new SimpleMeterRegistry();
        subscriptionRepublishingHolder = new SubscriptionRepublishingHolder();
        workerService = mock(WorkerService.class);


        healthCheckRestClient = mock(HealthCheckRestClient.class);
        when(environment.getActiveProfiles()).thenReturn(new String[]{"test"});

        when(polarisConfig.getRequestCooldownResetMins()).thenReturn(90);
        when(polarisConfig.getRepublishingBatchSize()).thenReturn(10);
        when(polarisConfig.getPollingBatchSize()).thenReturn(10);
        when(polarisConfig.getSuccessfulStatusCodes()).thenReturn(Arrays.asList(200, 201, 202, 204));

        when(polarisConfig.getRepublishingThreadpoolMaxPoolSize()).thenReturn(1);
        when(polarisConfig.getRepublishingThreadpoolCorePoolSize()).thenReturn(1);
        when(polarisConfig.getRepublishingThreadpoolQueueCapacity()).thenReturn(100);

        when(polarisConfig.getSubscriptionCheckThreadpoolMaxPoolSize()).thenReturn(1);
        when(polarisConfig.getSubscriptionCheckThreadpoolCorePoolSize()).thenReturn(1);
        when(polarisConfig.getSubscriptionCheckThreadpoolQueueCapacity()).thenReturn(100);

        when(threadPoolService.getKafkaTemplate()).thenReturn(kafkaTemplate);
        when(threadPoolService.getPartialSubscriptionCache()).thenReturn(partialSubscriptionCache);
        when(threadPoolService.getHealthCheckCache()).thenReturn(healthCheckCache);
        when(threadPoolService.getPolarisConfig()).thenReturn(polarisConfig);
        when(threadPoolService.getCircuitBreakerCacheService()).thenReturn(circuitBreakerCache);
        when(threadPoolService.getMessageStateMongoRepo()).thenReturn(messageStateMongoRepo);
        when(threadPoolService.getRestClient()).thenReturn(healthCheckRestClient);
        when(threadPoolService.getEventWriter()).thenReturn(eventWriter);

        var fakeStatusLine = mock(StatusLine.class);
        when(fakeStatusLine.getStatusCode()).thenReturn(200);
        when(fakeStatusLine.getReasonPhrase()).thenReturn("Ok");
        when(healthCheckRestClient.get(any(), any(), any(), any())).thenReturn(fakeStatusLine);
        when(healthCheckRestClient.head(any(), any(), any(), any())).thenReturn(fakeStatusLine);


        circuitBreakerManager = spy(new CircuitBreakerManager(threadPoolService));

        when(tracer.startScopedSpan(any())).thenReturn( mock(ScopedSpan.class));
        when(tracer.startSpanFromKafkaHeaders(anyString(), any())).thenReturn(mock(Span.class));
        when(threadPoolService.getTracer()).thenReturn(tracer);

        when(kafkaTemplate.send((ProducerRecord) any())).thenReturn(mock(CompletableFuture.class));

        threadPoolService = spy(new ThreadPoolService(circuitBreakerCache, healthCheckCache, partialSubscriptionCache, kafkaTemplate, polarisConfig, healthCheckRestClient, tracer, messageStateMongoRepo, eventWriter, meterRegistry, subscriptionRepublishingHolder, workerService));

        return threadPoolService;
    }

    public static List<CircuitBreakerMessage> createFakeCircuitBreakerMessages(int count) {
        return createFakeCircuitBreakerMessages(count, false, false);
    }

    public static List<CircuitBreakerMessage> createFakeCircuitBreakerMessages(int count, boolean randomSubscriptionIds) {
        return createFakeCircuitBreakerMessages(count, randomSubscriptionIds, false);
    }


    public static List<CircuitBreakerMessage> createFakeCircuitBreakerMessages(int count, boolean randomSubscriptionIds, boolean randomCallbackUrls) {
        var fakeCircuitBreakerMessages = new ArrayList<CircuitBreakerMessage>();
        for (int i = 0; i < count; i++) {
            var subscriptionId = randomSubscriptionIds ? RandomStringUtils.random(12, true, true) : SUBSCRIPTION_ID;
            var callbackUrl = randomCallbackUrls ? "http://" + RandomStringUtils.random(6, true, false) + ".de" : CALLBACK_URL;

            fakeCircuitBreakerMessages.add(new CircuitBreakerMessage(subscriptionId, CircuitBreakerStatus.OPEN, callbackUrl, ENV));
        }
        return fakeCircuitBreakerMessages;
    }

    public static MessageStateMongoDocument toMessageStateMongoDocument(State state) {
        return new MessageStateMongoDocument(state.getUuid(),
                state.getCoordinates(),
                state.getStatus(),
                state.getEnvironment(),
                state.getDeliveryType(),
                state.getSubscriptionId(),
                state.getEvent(),
                state.getProperties(),
                state.getMultiplexedFrom(),
                state.getEventRetentionTime(),
                state.getTimestamp(),
                state.getModified(),
                state.getError(),
                state.getAppliedScopes(),
                state.getScopeEvaluationResult(),
                state.getConsumerEvaluationResult()
                );
    }

    public static State createFakeState(String environment, Status status, boolean randomSubscriptionIds) {
        var event = new Event();
        event.setId(RandomStringUtils.random(12, true, true));

        String subscriptionId = randomSubscriptionIds ? RandomStringUtils.random(12, true, true) : SUBSCRIPTION_ID;
        String multiplexedFrom = RandomStringUtils.random(12, true, true);
        var eventMessage = new SubscriptionEventMessage(event, environment, DeliveryType.CALLBACK, subscriptionId, multiplexedFrom, EventRetentionTime.DEFAULT, List.of(), new HashMap<>(), new HashMap<>());
        eventMessage.getAdditionalFields().put(StateProperty.SUBSCRIBER_ID.getPropertyName(), RandomStringUtils.random(12, true, true));

        return State
                .builder(status, eventMessage, null, null)
                .coordinates( new Coordinates(RandomUtils.nextInt(0, 16), RandomUtils.nextInt()) )
                .timestamp(Date.from(Instant.now().minusSeconds(10)))
                .modified(Date.from(Instant.now().minusSeconds(10)))
                .build();
    }

    public static List<State> createFakeStates(int count, String environment, Status status, boolean randomSubscriptionIds) {
        var states = new ArrayList<State>();
        for (int i = 0; i < count; i++) {
            states.add(createFakeState(environment, status, randomSubscriptionIds));
        }
        return states;
    }

    public static List<MessageStateMongoDocument> createFakeMessageStateMongoDocuments(int count, String environment, Status status, boolean randomSubscriptionIds) {
        var states = createFakeStates(count, environment, status, randomSubscriptionIds);
        return states.stream().map(MockGenerator::toMessageStateMongoDocument).toList();
    }

    public static SubscriptionEventMessage createFakeSubscriptionEventMessage(DeliveryType deliveryType, boolean withAdditionalFields) {
        var sem = new SubscriptionEventMessage();

        sem.setUuid(EVENT_ID);
        sem.setEnvironment(ENV);
        sem.setSubscriptionId(SUBSCRIPTION_ID);
        sem.setDeliveryType(deliveryType);
        if (withAdditionalFields) {
            sem.setAdditionalFields(Map.of("callback-url", "foo.tld/bar"));
        }
        sem.setHttpHeaders(getAllHttpHeaders());

        return sem;
    }

    public static SubscriptionEventMessage createFakeSubscriptionEventMessage(String subscriptionId, String eventType) {
        Event event = new Event();
        event.setType(eventType);
        event.setData("""
                {
                    "myfancydata": "foo"
                }
                """);

        var subscriptionMessage = new SubscriptionEventMessage();
        subscriptionMessage.setSubscriptionId(subscriptionId);
        subscriptionMessage.setMultiplexedFrom(UUID.randomUUID().toString());
        subscriptionMessage.setDeliveryType(DeliveryType.CALLBACK);
        subscriptionMessage.setEnvironment("playground");
        subscriptionMessage.setUuid(UUID.randomUUID().toString());
        subscriptionMessage.setHttpHeaders(Map.of());
        subscriptionMessage.setEvent(event);
        return subscriptionMessage;
    }


    public static ConsumerRecord<String, String> createFakeConsumerRecord(SubscriptionEventMessage subscriptionEventMessage) throws JsonProcessingException {
        var objectmapper = new ObjectMapper();
        String json = objectmapper.writeValueAsString(subscriptionEventMessage);
        return new ConsumerRecord<>(TOPIC, 0, 0L, subscriptionEventMessage.getUuid(), json);
    }

    public static ConsumerRecord<String, String> createFakeConsumerRecord(DeliveryType deliveryType) throws JsonProcessingException {
        var msg = createFakeSubscriptionEventMessage(deliveryType, true);
        return createFakeConsumerRecord(msg);
    }

    private static Map<String, List<String>> getAllHttpHeaders() {
        var httpHeaders = new HashMap<String, List<String>>();
        httpHeaders.putAll(getExternalHeaders());
        httpHeaders.putAll(getInternalHttpHeaders());

        return httpHeaders;
    }

    public static Map<String, List<String>> getExternalHeaders() {
        var httpHeaders = new HashMap<String, List<String>>();
        httpHeaders.put("x-event-id", List.of(EVENT_ID));
        httpHeaders.put("x-event-type", List.of("pandora.smoketest.aws.v1"));
        httpHeaders.put("x-pubsub-publisher-id", List.of("eni--pandora--pandora-smoketest-aws-publisher"));
        httpHeaders.put("x-pubsub-subscriber-id", List.of("eni--pandora--pandora-smoketest-aws-subscriber-01"));

        return httpHeaders;
    }

    public static Map<String, List<String>> getInternalHttpHeaders() {
        var httpHeaders = new HashMap<String, List<String>>();

        //internal headers
        httpHeaders.put("x-spacegate-token", List.of("xxx-xxx-xxx"));
        httpHeaders.put("authorization", List.of("granted"));
        httpHeaders.put("content-length", List.of("high"));
        httpHeaders.put("host", List.of("tester"));
        httpHeaders.put("accept.test", List.of("yes"));
        httpHeaders.put("x-forwarded.test", List.of("stargate"));

        return httpHeaders;
    }

    public static PartialSubscription createFakePartialSubscription(DeliveryType deliveryType, boolean isGetMethodInsteadOfHead) {
        return createFakePartialSubscription(deliveryType, isGetMethodInsteadOfHead, false, false);
    }

    public static PartialSubscription createFakePartialSubscription(DeliveryType deliveryType, boolean isGetMethodInsteadOfHead, boolean newCallbackUrl) {
        return createFakePartialSubscription(deliveryType, isGetMethodInsteadOfHead, newCallbackUrl, false);
    }

    public static PartialSubscription createFakePartialSubscription(DeliveryType deliveryType, boolean isGetMethodInsteadOfHead, boolean newCallbackUrl,  boolean randomSubscriptionId) {
        return createFakePartialSubscription(deliveryType, isGetMethodInsteadOfHead, false, newCallbackUrl, randomSubscriptionId);
    }

    public static PartialSubscription createFakePartialSubscription(DeliveryType deliveryType, boolean isGetMethodInsteadOfHead, boolean isCircuitBreakerOptOut, boolean newCallbackUrl,  boolean randomSubscriptionId) {
        String subscriptionId = randomSubscriptionId ? RandomStringUtils.random(6, true, true) : SUBSCRIPTION_ID;
        String publisherId = PUBLISHER_ID;
        String subscriberId = SUBSCRIBER_ID;

        if (!deliveryType.equals(DeliveryType.CALLBACK)) {
            return new PartialSubscription(ENV, subscriptionId, publisherId, subscriberId, null, deliveryType, isGetMethodInsteadOfHead, isCircuitBreakerOptOut);
        } else {
            if (newCallbackUrl) {
                return new PartialSubscription(ENV, subscriptionId, publisherId, subscriberId, CALLBACK_URL_NEW, deliveryType, isGetMethodInsteadOfHead, isCircuitBreakerOptOut);
            } else {
                return new PartialSubscription(ENV, subscriptionId, publisherId, subscriberId, CALLBACK_URL, deliveryType, isGetMethodInsteadOfHead, isCircuitBreakerOptOut);
            }
        }
    }

    public static SubscriptionResource createFakeSubscriptionResource(final String environment, final String eventType) {
        var subscriptionRes = new SubscriptionResource();
        var spec = new SubscriptionResourceSpec();
        spec.setEnvironment(environment);
        var subscription = new Subscription();
        subscription.setSubscriptionId(UUID.randomUUID().toString());
        subscription.setSubscriberId(UUID.randomUUID().toString());
        subscription.setCallback("https://localhost:4711/foobar");
        subscription.setType(eventType);
        subscription.setDeliveryType("callback");
        subscription.setPayloadType("data");
        subscription.setPublisherId(UUID.randomUUID().toString());
        spec.setSubscription(subscription);

        subscriptionRes.setSpec(spec);

        return subscriptionRes;
    }


}
