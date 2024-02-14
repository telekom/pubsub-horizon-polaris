// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CouldNotDetermineWorkingSetException;
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
import java.util.Date;

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
     * @throws CouldNotDetermineWorkingSetException If there is an issue determining the working set.
     */
    @Scheduled(fixedDelayString = "${polaris.polling.interval-ms}", initialDelayString = "${random.int(${polaris.polling.interval-ms})}")
    public void run() throws CouldNotDetermineWorkingSetException {
        log.info("Start ScheduledEventDeliveringHandler");

        if(!polarisService.areResourcesFullySynced()) {
            log.info("Resources are not fully synced yet. Waiting for next run...");
            return;
        }

        boolean areWePodZero = determinePodIndex();
        if(!areWePodZero) {
            log.info("This pod ({}) is not first pod. Therefore not working on MessageStates in DELIVERING, skipping...", polarisConfig.getPodName());
            return;
        }

        int batchSize = polarisConfig.getPollingBatchSize();
        Pageable pageable = PageRequest.of(0, batchSize, Sort.by(Sort.Direction.ASC, "timestamp"));

        Date upperThresholdTimestamp = Date.from(Instant.now().minus(polarisConfig.getDeliveringStatesOffsetMins(), ChronoUnit.MINUTES));
        log.debug("DELIVERING timestamp upper threshold: {}", upperThresholdTimestamp);
        log.debug("pageable: {}", pageable);
        Slice<MessageStateMongoDocument> messageStatesSlices;

        do {
            messageStatesSlices = messageStateMongoRepo.findByDeliveryTypeAndStatusAndModifiedLessThanEqual(DeliveryType.CALLBACK, Status.DELIVERING, upperThresholdTimestamp, pageable);
            log.debug("messageStatesSlices: {} | {}", messageStatesSlices, messageStatesSlices.get().toList());

            if(messageStatesSlices.getNumberOfElements() > 0) {
                threadPoolService.startRepublishTask(messageStatesSlices);
            }
            pageable = pageable.next();

        } while(messageStatesSlices.hasNext());

        log.info("Finished ScheduledEventDeliveringHandler");
    }

    /**
     * Determines whether the current pod is the first pod based on the list of all pods.
     *
     * @return {@code true} if the current pod is the first pod, otherwise {@code false}.
     * @throws CouldNotDetermineWorkingSetException If there is an issue determining the pods.
     */
    private boolean determinePodIndex() throws CouldNotDetermineWorkingSetException {
        var allPods = podService.getAllPods();
        var ourPod = polarisConfig.getPodName();
        var index = allPods.indexOf(ourPod);

        return index == 0;
    }

}
