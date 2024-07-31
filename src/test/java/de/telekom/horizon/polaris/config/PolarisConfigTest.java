// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.config;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import de.telekom.horizon.polaris.util.EmbeddedKafkaHolder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static de.telekom.horizon.polaris.util.WiremockStubs.stubOidc;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class PolarisConfigTest {

    public static final EmbeddedKafkaBroker broker = EmbeddedKafkaHolder.getEmbeddedKafka();

    @RegisterExtension
    public static WireMockExtension wireMockServer = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    @Autowired
    PolarisConfig polarisConfig;

    @Test
    void getSubscriptionCheckThreadpoolQueueCapacity() {
        assertEquals(Integer.MAX_VALUE, polarisConfig.getSubscriptionCheckThreadpoolQueueCapacity());
    }

    @Test
    void getRepublishingThreadpoolQueueCapacity() {
        assertEquals(Integer.MAX_VALUE, polarisConfig.getRepublishingThreadpoolQueueCapacity());
    }

    @Test
    void getSubscriptionCheckThreadpoolMaxPoolSize() {
        assertEquals(Integer.MAX_VALUE, polarisConfig.getSubscriptionCheckThreadpoolMaxPoolSize());
    }

    @Test
    void getRepublishingThreadpoolMaxPoolSize() {
        assertEquals(Integer.MAX_VALUE, polarisConfig.getRepublishingThreadpoolMaxPoolSize());
    }

    @BeforeAll
    static void beforeAll() {
        stubOidc(wireMockServer);
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {

        registry.add("polaris.subscription-check.threadpool.queue-capacity", () -> "");
        registry.add("polaris.republish.threadpool.queue-capacity", () -> "");
        registry.add("polaris.subscription-check.threadpool.max-size", () -> "");
        registry.add("polaris.republish.threadpool.max-size", () -> "");

        registry.add("spring.zipkin.enabled", () -> false);
        registry.add("spring.zipkin.baseUrl", () -> "http://localhost:9411");
        registry.add("horizon.kafka.bootstrapServers", broker::getBrokersAsString);
        registry.add("horizon.kafka.partitionCount", () -> 1);
        registry.add("horizon.kafka.maxPollRecords", () -> 1);
        registry.add("horizon.kafka.autoCreateTopics", () -> true);
        registry.add("horizon.cache.kubernetesServiceDns", () -> "");
        registry.add("horizon.cache.deDuplication.enabled", () -> true);
        registry.add("kubernetes.enabled", () -> false);
        registry.add("polaris.polling.interval-ms", () -> 1000000000); // so big it does not get called
        registry.add("polaris.deliveringStates-offset-mins", () -> 0); // timestamp should be > 0 - now
        registry.add("logging.level.root", () -> "INFO");
        registry.add("polaris.oidc.token-uri", () -> wireMockServer.baseUrl() + "/oidc");
        registry.add("polaris.oidc.cronTokenFetch", () -> "-");
        registry.add("logging.level.de.telekom.horizon.polaris", () -> "DEBUG");
    }


}