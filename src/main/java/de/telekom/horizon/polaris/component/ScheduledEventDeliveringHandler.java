// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.service.PolarisService;
import de.telekom.horizon.polaris.service.PodService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scheduled task responsible for delivering events in the DELIVERING state to their respective subscribers.
 * <p>
 * This component queries the {@link MessageStateMongoRepo} for events in the DELIVERING state that are ready to be delivered.
 * If the pod is the first pod (index 0) in the list of all pods, it starts a republish task for the retrieved message states.
 * This task is responsible for republishing the events to their subscribers and updating their status accordingly.
 * </p>
 * <p>
 * The polling interval and batch size for processing DELIVERING events are configured in the {@link PolarisConfig}.
 * </p>
 *
 * @since 3.0
 */
@Slf4j
@Component
public class ScheduledEventDeliveringHandler {
    private final ThreadPoolService threadPoolService;
    private final MessageStateMongoRepo messageStateMongoRepo;
    private final PolarisConfig polarisConfig;
    private final PolarisService polarisService;
    private final PodService podService;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public ScheduledEventDeliveringHandler(ThreadPoolService threadPoolService, PolarisService polarisService) {
        this.threadPoolService = threadPoolService;
        this.messageStateMongoRepo = threadPoolService.getMessageStateMongoRepo();
        this.polarisConfig = threadPoolService.getPolarisConfig();
        this.polarisService = polarisService;
        this.podService = threadPoolService.getPodService();
    }

    /**
     * The scheduled task method that runs periodically to handle events in the DELIVERING state.
     * <p>
     * If resources are not fully synced or the pod is not the first pod, the method skips processing DELIVERING events.
     * Otherwise, it queries the {@link MessageStateMongoRepo} for events in the DELIVERING state ready for delivery.
     * It then starts a republish task for the retrieved message states.
     * </p>
     *
     */
    @Scheduled(fixedDelayString = "${polaris.polling.interval-ms}", initialDelayString = "${random.int(${polaris.polling.interval-ms})}")
    public void run() {
        if (isRunning.get()) {
            log.info("ScheduledEventDeliveringHandler is already running. Skipping this run...");
            return;
        }
        isRunning.set(true);
        log.info("Start ScheduledEventDeliveringHandler");

        if(!polarisService.areResourcesFullySynced()) {
            log.info("Resources are not fully synced yet. Waiting for next run...");
            isRunning.set(false);
            return;
        }

        boolean areWePodZero = podService.areWePodZero();
        if(!areWePodZero) {
            log.info("This pod ({}) is not first pod. Therefore not working on MessageStates in DELIVERING, skipping...", polarisConfig.getPodName());
            isRunning.set(false);
            return;
        }

        int batchSize = polarisConfig.getPollingBatchSize();
        Pageable pageable = PageRequest.of(0, batchSize, Sort.by(Sort.Direction.ASC, "timestamp"));

        Date upperThresholdTimestamp = Date.from(Instant.now().minus(polarisConfig.getDeliveringStatesOffsetMins(), ChronoUnit.MINUTES));
        log.debug("DELIVERING timestamp upper threshold: {}", upperThresholdTimestamp);
        log.debug("pageable: {}", pageable);
        Slice<MessageStateMongoDocument> messageStatesSlices;

        List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();

        do {
            messageStatesSlices = messageStateMongoRepo.findByDeliveryTypeAndStatusAndModifiedLessThanEqual(DeliveryType.CALLBACK, Status.DELIVERING, upperThresholdTimestamp, pageable);
            log.debug("messageStatesSlices: {} | {}", messageStatesSlices, messageStatesSlices.get().toList());

            if(messageStatesSlices.getNumberOfElements() > 0) {
                CompletableFuture<Void> republishTask = threadPoolService.startRepublishTask(messageStatesSlices);
                if (republishTask != null) {
                    completableFutureList.add(republishTask);
                }
            }
            pageable = pageable.next();

        } while(messageStatesSlices.hasNext());

        // wait for tasks to complete to really finish the run
        for(CompletableFuture<Void> completableFuture : completableFutureList) {
            try {
                completableFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Unexpected error processing event task", e);
            }
        }

        log.info("Finished ScheduledEventDeliveringHandler");
        isRunning.set(false);
    }
}
