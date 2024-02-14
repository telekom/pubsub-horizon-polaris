package de.telekom.horizon.polaris.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@Getter
public class PolarisConfig {
    @Value("${kubernetes.informer.namespace}")
    private String environment;

    @Value("${polaris.pod-name}")
    private String podName;

    @Value("${polaris.max-timeout}")
    private long maxTimeout;

    @Value("${polaris.max-connections}")
    private int maxConnections;

    @Value("${polaris.picking.timeout-ms}")
    private int pickingTimeoutMs;

    @Value("${polaris.polling.interval-ms}")
    private int pollingIntervalMs;
    @Value("${polaris.polling.batch-size}")
    private int pollingBatchSize;
    @Value("${polaris.request.threadpool.pool-size}")
    private int requestThreadpoolPoolSize;
    @Value("${polaris.request.delay-mins}")
    private int requestDelayInbetweenMins;
    @Value("${polaris.request.cooldown-reset-mins}")
    private int requestCooldownResetMins;

    @Value("${polaris.subscription-check.threadpool.max-size}")
    private int subscriptionCheckThreadpoolMaxPoolSize;
    @Value("${polaris.subscription-check.threadpool.core-size}")
    private int subscriptionCheckThreadpoolCorePoolSize;
    @Value("${polaris.subscription-check.threadpool.queue-capacity}")
    private int subscriptionCheckThreadpoolQueueCapacity;

    @Value("${polaris.republish.threadpool.max-size}")
    private int republishingThreadpoolMaxPoolSize;
    @Value("${polaris.republish.threadpool.core-size}")
    private int republishingThreadpoolCorePoolSize;
    @Value("${polaris.republish.threadpool.queue-capacity}")
    private int republishingThreadpoolQueueCapacity;

    @Value("${polaris.republish.batch-size}")
    private int republishingBatchSize;
    @Value("${polaris.republish.timeout-ms}")
    private int republishingTimeoutMs;
    @Value("${polaris.deliveringStates-offset-mins}")
    private int deliveringStatesOffsetMins;

    @Value("#{'${polaris.request.successful-status-codes}'.split(',')}")
    private List<Integer> successfulStatusCodes;
}
