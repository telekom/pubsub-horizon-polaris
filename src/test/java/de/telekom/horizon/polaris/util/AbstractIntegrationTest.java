// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.kubernetes.InformerStoreInitHandler;
import de.telekom.eni.pandora.horizon.kubernetes.PodResourceListener;
import de.telekom.eni.pandora.horizon.kubernetes.SubscriptionResourceListener;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.cache.PolarisPodCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static de.telekom.horizon.polaris.util.WiremockStubs.stubOidc;

@SpringBootTest()
@Import(MongoTestServerConfiguration.class)
@Slf4j
public abstract class AbstractIntegrationTest {

    static {
        EmbeddedKafkaHolder.getEmbeddedKafka();
    }

    public static final EmbeddedKafkaBroker broker = EmbeddedKafkaHolder.getEmbeddedKafka();

    @RegisterExtension
    public static WireMockExtension wireMockServer = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    @Autowired
    private MessageStateMongoRepo messageStateMongoRepo;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private EventWriter eventWriter;

    @Autowired
    private CircuitBreakerCacheService circuitBreakerCacheService;

    @Autowired
    protected PartialSubscriptionCache partialSubscriptionCache;
    @Autowired
    protected PolarisConfig polarisConfig;

    @Autowired
    protected PolarisPodCache polarisPodCache;

    @MockBean
    private SubscriptionResourceListener subscriptionResourceListener;
    @MockBean
    protected PodResourceListener podResourceListener;
    @MockBean
    private InformerStoreInitHandler informerStoreInitHandler;

    @Autowired
    protected HealthCheckCache healthCheckCache;

    @Autowired
    private ConsumerFactory consumerFactory;

    private static final Map<String, BlockingQueue<ConsumerRecord<String, String>>> multiplexedRecordsMap = new HashMap<>();

    private KafkaMessageListenerContainer<String, String> container;

    private String eventType;

    @BeforeEach
    void setUp() {
        eventType = "junit.test.event." + DigestUtils.sha1Hex(String.valueOf(System.currentTimeMillis()));

        multiplexedRecordsMap.putIfAbsent(getEventType(), new LinkedBlockingQueue<>());

        var pods = polarisPodCache.getAllPods();
        pods.forEach(polarisPodCache::remove);
        polarisPodCache.add( polarisConfig.getPodName() );

        var topicNames = Arrays.stream(EventRetentionTime.values()).map(EventRetentionTime::getTopic).toArray(String[]::new);
        var containerProperties = new ContainerProperties(topicNames);
        containerProperties.setGroupId("test-consumer-groupid"+getEventType());
        containerProperties.setAckMode(ContainerProperties.AckMode.RECORD);
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setBeanName("test-consumer-container");
        container.setupMessageListener((MessageListener<String, String>) record -> multiplexedRecordsMap.get(getEventType()).add(record));
        container.start();

        ContainerTestUtils.waitForAssignment(container, (EventRetentionTime.values().length - 1) * broker.getPartitionsPerTopic());
    }

    @AfterEach
    void tearDown() {
        multiplexedRecordsMap.clear();
        if(container != null) { container.stop(); }
        messageStateMongoRepo.deleteAll();
        wireMockServer.resetAll();
        Collections.list(healthCheckCache.getAllKeys()).forEach(callbackKey -> healthCheckCache.remove(callbackKey.callbackUrl(), callbackKey.httpMethod()));
        // Close circuit breakers
        var cbs = circuitBreakerCacheService.getCircuitBreakerMessages();
        cbs.forEach(cb -> circuitBreakerCacheService.closeCircuitBreaker(cb.getSubscriptionId()));
    }

    public SendResult<String, String> simulateNewPublishedEvent(SubscriptionEventMessage message) throws JsonProcessingException, ExecutionException, InterruptedException {
        return eventWriter.send(Objects.requireNonNullElse(message.getEventRetentionTime(), EventRetentionTime.DEFAULT).getTopic(), message).get();
    }

    public void simulateOpenCircuitBreaker(PartialSubscription partialSubscription, String eventMessageUuid, int partition, long offset) {
        simulateCircuitBreaker(partialSubscription, eventMessageUuid, partition, offset, "", CircuitBreakerStatus.OPEN);
    }

    public void simulateRepublishingCircuitBreaker(PartialSubscription partialSubscription, String eventMessageUuid, int partition, long offset) {
        simulateCircuitBreaker(partialSubscription, eventMessageUuid, partition, offset, "", CircuitBreakerStatus.REPUBLISHING);
    }

    public void simulateCircuitBreaker(PartialSubscription partialSubscription, String eventMessageUuid, int partition, long offset, String assignedPod, CircuitBreakerStatus status) {
        CircuitBreakerMessage circuitBreakerMessage = new CircuitBreakerMessage(partialSubscription.subscriptionId(), status, partialSubscription.callbackUrl(),  partialSubscription.environment());
        circuitBreakerMessage.setAssignedPodId(assignedPod);
        circuitBreakerCacheService.updateCircuitBreakerMessage(circuitBreakerMessage);

        MessageStateMongoDocument testMessageStateMongoDocumentWaiting = MockGenerator.createFakeMessageStateMongoDocuments(1, partialSubscription.environment(), Status.WAITING, false).get(0);
        testMessageStateMongoDocumentWaiting.setUuid(eventMessageUuid);
        testMessageStateMongoDocumentWaiting.setCoordinates(partition, offset);
        testMessageStateMongoDocumentWaiting.setSubscriptionId(partialSubscription.subscriptionId());
        messageStateMongoRepo.save(testMessageStateMongoDocumentWaiting);
    }

    public PartialSubscription addTestSubscription(SubscriptionResource subscriptionResource) {
        var pa =  PartialSubscription.fromSubscriptionResource(subscriptionResource);
        partialSubscriptionCache.add(pa);
        return pa;
    }

    public ConsumerRecord<String, String> pollForRecord(int timeout, TimeUnit timeUnit) throws InterruptedException {
        return multiplexedRecordsMap.get(getEventType()).poll(timeout, timeUnit);
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.zipkin.enabled", () -> false);
        registry.add("spring.zipkin.baseUrl", () -> "http://localhost:9411");
        registry.add("horizon.kafka.bootstrapServers", broker::getBrokersAsString);
        registry.add("horizon.kafka.partitionCount", () -> 1);
        registry.add("horizon.kafka.maxPollRecords", () -> 1);
        registry.add("horizon.kafka.autoCreateTopics", () -> true);
        registry.add("horizon.cache.kubernetesServiceDns", () -> "");
        registry.add("horizon.cache.deDuplication.enabled", () -> true);
        registry.add("kubernetes.enabled", () -> false);
        registry.add("polaris.polling.interval-ms", () -> 1000000000);   // so big it does not get called
        registry.add("polaris.max-timeout", () -> 2000);
        registry.add("polaris.republish.timeout-ms", () -> 1000);
        registry.add("polaris.deliveringStates-offset-mins", () -> 0); // timestamp should be > 0 - now
        registry.add("logging.level.root", () -> "INFO");
        registry.add("polaris.oidc.token-uri", () -> wireMockServer.baseUrl() + "/oidc");
        registry.add("polaris.oidc.cronTokenFetch", () -> "-");
        registry.add("logging.level.de.telekom.horizon.polaris", () -> "DEBUG");
    }

    @BeforeAll
    static void beforeAll() {
        stubOidc(wireMockServer);
    }

    public String getEventType() {
        return eventType;
    }

}
