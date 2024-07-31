// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.service;

import com.google.common.util.concurrent.*;
import de.telekom.eni.pandora.horizon.cache.listener.SubscriptionResourceEvent;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.component.HealthCheckRestClient;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.model.CallbackKey;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.task.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Slice;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * This class has maintains 3 pools (2 x normal, 1 x scheduled). The pools are for
 * <ol>
 *     <li>{@link SubscriptionComparisonTask} comparing an old subscription against a new one and creating either a {@link HealthRequestTask} or {@link RepublishingTask}</li>
 *     <li>{@link HealthRequestTask} makes the (scheduled) head/get requests to the callbackUrl</li>
 *     <li>{@link RepublishingTask} republishes all events for a given callbackUrl or subscription</li>
 * </ol>
 *
 * @author Tim Pütz
 * @since 3.0
 */
@Getter
@Slf4j
@Component
public class ThreadPoolService {
    @Getter(AccessLevel.PRIVATE)
    private final ThreadPoolTaskExecutor subscriptionCheckTaskExecutor;
    @Getter(AccessLevel.PRIVATE)
    private final ThreadPoolTaskExecutor republishingTaskExecutor;
    @Getter(AccessLevel.PRIVATE)
    private final ListeningScheduledExecutorService requestTaskScheduler;

    private final CircuitBreakerCacheService circuitBreakerCacheService;
    private final HealthCheckCache healthCheckCache;
    private final PartialSubscriptionCache partialSubscriptionCache;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final PolarisConfig polarisConfig;
    private final HealthCheckRestClient restClient;
    private final HorizonTracer tracer;
    private final MessageStateMongoRepo messageStateMongoRepo;
    private final ConcurrentHashMap<CallbackKey, ListenableScheduledFuture<?>> requestingTasks;
    private final EventWriter eventWriter;
    private final MeterRegistry meterRegistry;
    private final SubscriptionRepublishingHolder subscriptionRepublishingHolder;
    private final WorkerService workerService;

    public ThreadPoolService(CircuitBreakerCacheService circuitBreakerCacheService,
                             HealthCheckCache healthCheckCache,
                             PartialSubscriptionCache partialSubscriptionCache,
                             KafkaTemplate<String, String> kafkaTemplate,
                             PolarisConfig polarisConfig,
                             HealthCheckRestClient restClient,
                             HorizonTracer tracer,
                             MessageStateMongoRepo messageStateMongoRepo,
                             EventWriter eventWriter,
                             MeterRegistry meterRegistry,
                             SubscriptionRepublishingHolder subscriptionRepublishingHolder, WorkerService workerService) {
        this.circuitBreakerCacheService = circuitBreakerCacheService;
        this.restClient = restClient;
        this.healthCheckCache = healthCheckCache;
        this.partialSubscriptionCache = partialSubscriptionCache;
        this.kafkaTemplate = kafkaTemplate;
        this.polarisConfig = polarisConfig;
        this.tracer = tracer;
        this.messageStateMongoRepo = messageStateMongoRepo;
        this.eventWriter = eventWriter;
        this.meterRegistry = meterRegistry;
        this.subscriptionRepublishingHolder = subscriptionRepublishingHolder;
        this.workerService = workerService;

        this.republishingTaskExecutor = new ThreadPoolTaskExecutor();
        this.subscriptionCheckTaskExecutor = new ThreadPoolTaskExecutor();
        this.requestingTasks = new ConcurrentHashMap<>();

        // Need to do this, bc Spring does not have ScheduledFuture return values for their ThreadPoolScheduledTaskExecutor (issue: https://github.com/spring-projects/spring-framework/issues/17987)
        var scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(polarisConfig.getRequestThreadpoolPoolSize());
        scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true); // Set to true, else OutOfMemory can occur, when Task do not get removed
        this.requestTaskScheduler = MoreExecutors.listeningDecorator(scheduledThreadPoolExecutor);
        ExecutorServiceMetrics.monitor(meterRegistry, scheduledThreadPoolExecutor, "requestTaskScheduler", Collections.emptyList());

        initializeTaskExecutor();
    }

    private void initializeTaskExecutor() {
        republishingTaskExecutor.setMaxPoolSize(polarisConfig.getRepublishingThreadpoolMaxPoolSize());
        republishingTaskExecutor.setCorePoolSize(polarisConfig.getRepublishingThreadpoolCorePoolSize());
        republishingTaskExecutor.setQueueCapacity(polarisConfig.getRepublishingThreadpoolQueueCapacity());
        republishingTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        republishingTaskExecutor.afterPropertiesSet();
        ExecutorServiceMetrics.monitor(meterRegistry, republishingTaskExecutor.getThreadPoolExecutor(), "republishingTaskExecutor", Collections.emptyList());

        subscriptionCheckTaskExecutor.setMaxPoolSize(polarisConfig.getSubscriptionCheckThreadpoolMaxPoolSize());
        subscriptionCheckTaskExecutor.setCorePoolSize(polarisConfig.getSubscriptionCheckThreadpoolCorePoolSize());
        subscriptionCheckTaskExecutor.setQueueCapacity(polarisConfig.getSubscriptionCheckThreadpoolQueueCapacity());
        subscriptionCheckTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        subscriptionCheckTaskExecutor.afterPropertiesSet();
        ExecutorServiceMetrics.monitor(meterRegistry, subscriptionCheckTaskExecutor.getThreadPoolExecutor(), "subscriptionCheckTaskExecutor", Collections.emptyList());
    }

    private void handleRepublishingCallbackFinished(List<PartialSubscription> partialSubscriptions) {
        log.info("Successfully finished republishing task (RepublishPartialSubscriptions) for partialSubscriptions {}", partialSubscriptions);
        for (var partialSubscription: partialSubscriptions) {
            String callbackUrl = partialSubscription.callbackUrl();
            HttpMethod httpMethod = partialSubscription.isGetMethodInsteadOfHead() ? HttpMethod.GET : HttpMethod.HEAD;
            var key = new CallbackKey(callbackUrl, httpMethod);
            if (requestingTasks.containsKey(key)) {
                var healthCheckEntry = healthCheckCache.get(callbackUrl, httpMethod);
                if(healthCheckEntry.isPresent() && (healthCheckEntry.get().getSubscriptionIds().isEmpty())) {
                        log.warn("RequestTask for {} was found, but has no more subscription ids... Stopping old requestTask", key);
                        stopHealthRequestTask(callbackUrl, httpMethod);
                }
            }
        }
    }

    private void handleRequestFinished(ListenableScheduledFuture<Boolean> future, Boolean wasSuccessful, String callbackUrl, String environment, HttpMethod httpMethod, String publisherId, String subscriberId, @Nullable Throwable throwable) {
        var key = new CallbackKey(callbackUrl, httpMethod);
        requestingTasks.remove(key);

        var isCancelled = future.isCancelled(); // If StopRequestTask in threadPoolService gets called, this gets true
        if (isCancelled) {
            log.info("Thread got interrupted, will set isThreadOpen to false for callbackUrl: {} and httpMethod: {}", callbackUrl, httpMethod);
            // Set isThreadOpen to false
            healthCheckCache.update(callbackUrl, httpMethod, false);
        } else {
            if (Boolean.TRUE.equals(wasSuccessful)) {
                startHandleSuccessfulHealthRequestTask(callbackUrl, httpMethod);
            } else {
                startHealthRequestTask(callbackUrl, publisherId, subscriberId, environment, httpMethod);
            }
        }

        if (throwable != null) {
            log.error("Could not finish request task for callbackUrl {}, environment {} and httpMethod {}, with the following error", callbackUrl, environment, httpMethod, throwable);
            return;
        }

        log.info("Successfully finished request task for callbackUrl {}, environment {} and httpMethod {}", callbackUrl, environment, httpMethod);
    }

    public void startSubscriptionComparisonTask(PartialSubscription oldSubscription, PartialSubscription currentSubscriptionOrNull) {
        log.info("Starting subscription comparison task for subscriptions (old -> current) {} -> {}", oldSubscription, currentSubscriptionOrNull);
        var subscriptionCheckTask = new SubscriptionComparisonTask(oldSubscription, currentSubscriptionOrNull, this);
        var future = subscriptionCheckTaskExecutor.submitCompletable(subscriptionCheckTask);
        future.exceptionally(throwable -> {
            log.error("Could not finish subscription comparison task for subscriptions (old -> current) {} -> {} with the following error", oldSubscription, currentSubscriptionOrNull, throwable);
            return null;
        });
        future.thenAccept(result -> log.info("Successfully finished subscription comparison task for subscriptions (old -> current) {} -> {}", oldSubscription, currentSubscriptionOrNull));
    }

    public CompletableFuture<Void> startRepublishTask(Slice<MessageStateMongoDocument> messageStateMongoDocuments) {
        log.info("Starting republishing task for  {} messageStateMongoDocuments", messageStateMongoDocuments.getNumberOfElements());
        var republishingTask = new RepublishingTask(messageStateMongoDocuments, this);
        var future = republishingTaskExecutor.submitCompletable(republishingTask);
        future.exceptionally(throwable -> {
            log.error("Could not finish republish task for {} messageStateMongoDocuments with the following error", messageStateMongoDocuments.getNumberOfElements(), throwable);
            return null;
        });
        future.thenAccept(result -> log.info("Successfully finished republishing task for {} messageStateMongoDocuments", messageStateMongoDocuments.getNumberOfElements()));
        return future;
    }

    public void startHandleDeliveryTypeChangeTask(PartialSubscription newPartialSubscription) {
        log.info("Subscription Change detected! Starting republishing task for newPartialSubscription {}", newPartialSubscription);
        var republishingTask = new HandleDeliveryTypeChangeTask(newPartialSubscription, this);
        var future = republishingTaskExecutor.submitCompletable(republishingTask);
        future.exceptionally(throwable -> {
            log.error("Could not finish republish task (HandleDeliveryTypeChangeTask) for subscriptionId {} with the following error", newPartialSubscription.subscriptionId(), throwable);
            return null;
        });
        future.thenAccept(result -> log.info("Successfully finished republishing task (HandleDeliveryTypeChangeTask) for subscriptionId {}", newPartialSubscription.subscriptionId()));
    }

    public void startHandleSuccessfulHealthRequestTask(String callbackUrl, HttpMethod httpMethod) {
        log.info("Successful HealthRequest! Starting republishing task for callbackUrl {} and httpMethod {}", callbackUrl, httpMethod);
        var republishingTask = new HandleSuccessfulHealthRequestTask(callbackUrl, httpMethod, this);
        var future = republishingTaskExecutor.submitCompletable(republishingTask);
        future.exceptionally(throwable -> {
            log.error("Could not finish republish task (HandleSuccessfulHealthRequestTask) for callbackUrl {}  and httpMethod {}, with the following error", callbackUrl, httpMethod, throwable);
            return null;
        });
        future.thenAccept(result -> log.info("Successfully finished republishing task (HandleSuccessfulHealthRequestTask) for callbackUrl {} and httpMethod {}", callbackUrl, httpMethod));
    }

    public CompletableFuture<Void> startRepublishSubscriptionIdsTask(List<PartialSubscription> partialSubscriptions) {
        log.info("Successful HealthRequest! Starting republishing task for partialSubscriptions {}", partialSubscriptions);
        var republishingTask = new RepublishPartialSubscriptionsTask(partialSubscriptions, this);
        var future = republishingTaskExecutor.submitCompletable(republishingTask);
        future.exceptionally(throwable -> {
            log.error("Could not finish republish task (RepublishPartialSubscriptions) for partialSubscriptions {} , with the following error", partialSubscriptions, throwable);
            return null;
        });
        future.thenAccept(result -> this.handleRepublishingCallbackFinished(partialSubscriptions));
        return future;
    }

    public void startHealthRequestTask(String callbackUrl, String publisherId, String subscriberId, String environment, HttpMethod httpMethod) {
        log.info("Starting HealthRequest task for callbackUrl {}, environment {} and httpMethod {},", callbackUrl, environment, httpMethod);
        Duration delay = Duration.ofMinutes(polarisConfig.getRequestDelayInbetweenMins());
        this.startHealthRequestTask(callbackUrl, publisherId, subscriberId, environment, httpMethod, delay);
    }

    public ListenableScheduledFuture<Boolean> startHealthRequestTask(String callbackUrl, String publisherId, String subscriberId, String environment, HttpMethod httpMethod, Duration initialDelay) {
        log.info("Starting HealthRequest task with initialDelay {} for callbackUrl {}, environment {} and httpMethod {}", initialDelay, callbackUrl, environment, httpMethod);

        var key = new CallbackKey(callbackUrl, httpMethod);
        if (requestingTasks.containsKey(key)) {
            log.warn("RequestTask for {} is already running...this should not occur. Stopping old requestTask", key);
            stopHealthRequestTask(callbackUrl, httpMethod);
        }

        var healthRequestTask = new HealthRequestTask(callbackUrl, publisherId, subscriberId, environment, httpMethod, this);
        var future = requestTaskScheduler.schedule(healthRequestTask, initialDelay);

        requestingTasks.put(key, future);

        // Need to do this, bc there is no addCallback with success and failure Callbacks :)))))))
        Futures.addCallback(future, new FutureCallback<>() {
            @Override
            public void onSuccess(Boolean wasSuccessful) {
                handleRequestFinished(future, wasSuccessful, callbackUrl, environment, httpMethod, publisherId, subscriberId,null);
            }

            @Override
            public void onFailure(Throwable throwable) {
                handleRequestFinished(future, false, callbackUrl, environment, httpMethod, publisherId, subscriberId, throwable);
            }
        }, MoreExecutors.directExecutor()); // Careful: Do not use directExecutor on heavy-weight task.
        // We only can do it bc we are just doing logs. Will be executor in ThreadPoolService Thread (this)
        return future;
    }

    public void stopHealthRequestTask(String callbackUrl, HttpMethod httpMethod) {
        log.info("Stopping HealthRequest task for callbackUrl {}  and httpMethod {}", callbackUrl, httpMethod);
        var key = new CallbackKey(callbackUrl, httpMethod);
        if (!requestingTasks.containsKey(key)) {
            log.warn("RequestTask for {} not found...this should not occur. Returning", key);
            return;
        }

        try {
            var future = requestingTasks.get(key);
            future.cancel(false); // We do not need to interrupt. Lets RequestTask finish and set isThreadOpen to false. Could also be done here
        } catch (Exception exception) {
            log.warn("Unexpected Exception while stopping health request task for callbackUrl {} and httpMethod: {}", callbackUrl, httpMethod, exception);
        }
    }

    public Optional<ListenableScheduledFuture<?>> getHealthRequestTask(String callbackUrl, HttpMethod httpMethod) {
        var key = new CallbackKey(callbackUrl, httpMethod);
        return Optional.ofNullable(requestingTasks.get(key));
    }

    @EventListener
    public void handleSubscriptionResourceEvent(SubscriptionResourceEvent event) {
        var eventType = event.getEntryEvent().getEventType();

        log.debug("Received {} event for subscription with ID {}", eventType, event.getEntryEvent().getKey());

        switch (event.getEntryEvent().getEventType()) {
            case UPDATED:
                var newResource = event.getValue();
                var oldResource = event.getOldValue();

                if (newResource != null && oldResource != null) {
                    log.debug("Update SubscriptionResource: {}", newResource);

                    var newPartialSubscription = PartialSubscription.fromSubscriptionResource(newResource);
                    var oldPartialSubscription = PartialSubscription.fromSubscriptionResource(oldResource);

                    log.debug("oldPartialSubscription: {} -> newPartialSubscription: {}", oldPartialSubscription, newPartialSubscription);
                    if(checkIfCallbackUrlOrHttpMethodChanged(oldPartialSubscription, newPartialSubscription)
                            || newPartialSubscription.isCircuitBreakerOptOut()) {

                        var oldHttpMethod = oldPartialSubscription.isGetMethodInsteadOfHead() ? HttpMethod.GET : HttpMethod.HEAD;
                        stopHealthRequestTask(oldPartialSubscription.callbackUrl(), oldHttpMethod);
                        startSubscriptionComparisonTask(oldPartialSubscription, newPartialSubscription);
                    }
                }
                break;
            case REMOVED:
            case EVICTED:
                var resource = event.getValue();
                if (resource != null) {
                    var partialSubscription = PartialSubscription.fromSubscriptionResource(resource);
                    startSubscriptionComparisonTask(partialSubscription, null);
                }

                break;
        }



    }

    /**
     * Checks if the callback URL or HTTP method has changed between old and new {@link PartialSubscription}.
     *
     * @param oldPartialSubscription The old state of the {@link PartialSubscription}.
     * @param newPartialSubscription The new state of the {@link PartialSubscription}.
     * @return {@code true} if either the callback URL or HTTP method has changed, otherwise {@code false}.
     */
    private boolean checkIfCallbackUrlOrHttpMethodChanged(PartialSubscription oldPartialSubscription, PartialSubscription newPartialSubscription) {
        // can be null if subscription was/is SSE
        String oldCallbackUrlOrNull = oldPartialSubscription.callbackUrl();
        String newCallbackUrlOrNull = newPartialSubscription.callbackUrl();

        boolean callbackUrlChanged = !Objects.equals(oldCallbackUrlOrNull, newCallbackUrlOrNull);
        boolean httpMethodChanged =  !Objects.equals(oldPartialSubscription.isGetMethodInsteadOfHead(), newPartialSubscription.isGetMethodInsteadOfHead());

        log.debug("callbackUrlChanged: {}", callbackUrlChanged);
        log.debug("httpMethodChanged: {}", httpMethodChanged);

        return callbackUrlChanged || httpMethodChanged;
    }
}
