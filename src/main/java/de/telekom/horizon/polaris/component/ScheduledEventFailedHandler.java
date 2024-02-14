// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

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

// On callback -> sse
// a) the multiplexer starts adressing the new events to the tasse and not to the dude -> no more WAITING or FAILED
// b) the dude puts the events with this subscription id that he is currently working on in failed
// c) the polaris recognises the susbcription change, gets all FAILED currently in the status db and reublishes them
//      -> all oncoming FAILED from the dude would be lost if we dont do this

/**
 * Scheduled task responsible for handling events in the FAILED state.
 * <p>
 * This component queries the {@link MessageStateMongoRepo} for events in the FAILED state.
 * If the pod is the first pod (index 0) in the list of all pods, it starts a republish task for the retrieved message states.
 * This task is responsible for republishing the events and updating their status accordingly.
 * </p>
 * <p>
 * The polling interval and batch size for processing FAILED events are configured in the {@link PolarisConfig}.
 * </p>
 *
 * @since 3.0
 */
@Slf4j
@Component
public class ScheduledEventFailedHandler {
    private final ThreadPoolService threadPoolService;
    private final MessageStateMongoRepo messageStateMongoRepo;
    private final PolarisConfig polarisConfig;
    private final PolarisService polarisService;
    private final PodService podService;

    public ScheduledEventFailedHandler(ThreadPoolService threadPoolService, PolarisService polarisService) {
        this.threadPoolService = threadPoolService;
        this.messageStateMongoRepo = threadPoolService.getMessageStateMongoRepo();
        this.polarisConfig = threadPoolService.getPolarisConfig();
        this.polarisService = polarisService;
        this.podService = threadPoolService.getPodService();
    }


    /**
     * The scheduled task method that runs periodically to handle events in the FAILED state.
     * <p>
     * If resources are not fully synced or the pod is not the first pod, the method skips processing FAILED events.
     * Otherwise, it queries the {@link MessageStateMongoRepo} for events in the FAILED state with a {@code CallbackUrlNotFoundException} and starts a republish task for them.
     * </p>
     *
     * @throws CouldNotDetermineWorkingSetException If there is an issue determining the working set.
     */
    @Scheduled(fixedDelayString = "${polaris.polling.interval-ms}", initialDelayString = "${random.int(${polaris.polling.interval-ms})}")
    public void run() throws CouldNotDetermineWorkingSetException {
        log.info("Start ScheduledEventFailedHandler");

        if(!polarisService.areResourcesFullySynced()) {
            log.info("Resources are not fully synced yet. Waiting for next run...");
            return;
        }

        boolean areWePodZero = determinePodIndex();
        if(!areWePodZero) {
            log.info("This pod ({}) is not first pod. Therefore not working on MessageStates in FAILED, skipping...", polarisConfig.getPodName());
            return;
        }

        int batchSize = polarisConfig.getPollingBatchSize();
        Pageable pageable = PageRequest.of(0, batchSize, Sort.by(Sort.Direction.ASC, "timestamp"));
        log.debug("pageable: {}", pageable);
        Slice<MessageStateMongoDocument> messageStatesSlices;
        do {
            messageStatesSlices = messageStateMongoRepo.findStatusFailedWithCallbackExceptionAsc(pageable);
            log.debug("messageStatesSlices: {} | {}", messageStatesSlices, messageStatesSlices.get().toList());

            if(messageStatesSlices.getNumberOfElements() > 0) {
                threadPoolService.startRepublishTask(messageStatesSlices);
            }

            pageable = pageable.next();
        } while(messageStatesSlices.hasNext());

        log.info("Finished ScheduledEventFailedHandler");
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
