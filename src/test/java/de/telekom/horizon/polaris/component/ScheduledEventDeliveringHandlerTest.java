// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.mongo.repository.MessageStateMongoRepo;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.service.WorkerService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.sql.Date;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.stream.Stream;

import static de.telekom.horizon.polaris.TestConstants.*;
import static de.telekom.horizon.polaris.util.MockGenerator.createFakeMessageStateMongoDocuments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Slf4j
class ScheduledEventDeliveringHandlerTest {

    ScheduledEventDeliveringHandler scheduledEventDeliveringHandler;

    @MockBean
    ThreadPoolService threadPoolService;

    @MockBean
    PartialSubscriptionCache partialSubscriptionCache;

    @MockBean
    PolarisConfig polarisConfig;

    @MockBean
    MessageStateMongoRepo messageStateMongoRepo;

    PartialSubscription fakePartialSubscription;

    @BeforeEach
    void prepare() {
        log.info("prepare");

        fakePartialSubscription = new PartialSubscription(ENV, SUBSCRIPTION_ID, PUBLISHER_ID, SUBSCRIBER_ID, CALLBACK_URL, DeliveryType.CALLBACK, false, false);


        when(polarisConfig.getPollingBatchSize()).thenReturn(10);
        when(polarisConfig.getDeliveringStatesOffsetMins()).thenReturn(30);

        when(threadPoolService.getPolarisConfig()).thenReturn(polarisConfig);
        when(threadPoolService.getPartialSubscriptionCache()).thenReturn(partialSubscriptionCache);
        when(threadPoolService.getMessageStateMongoRepo()).thenReturn(messageStateMongoRepo);
        when(threadPoolService.getWorkerService()).thenReturn(Mockito.mock(WorkerService.class));

        when(partialSubscriptionCache.get(eq(SUBSCRIPTION_ID))).thenReturn(Optional.ofNullable(fakePartialSubscription));

        var slices = createFakeSlices();
        when(messageStateMongoRepo.findByDeliveryTypeAndStatusAndModifiedLessThanEqual(any(), any(), any(), any(Pageable.class))).thenReturn(slices);

        scheduledEventDeliveringHandler = spy(new ScheduledEventDeliveringHandler(threadPoolService));

        when(threadPoolService.getWorkerService().tryGlobalLock()).thenReturn(true);
    }

    @Test
    @DisplayName("Handle republishing message depending on")
    void startRepublishingForMessageWhichExceedDeliveringOffset() {
        scheduledEventDeliveringHandler.run();

        verify(threadPoolService, times(1)).startRepublishTask(any(Slice.class));
    }

    Slice<MessageStateMongoDocument> createFakeSlices() {
        var docsOutOfTime = createFakeMessageStateMongoDocuments(10, ENV, Status.DELIVERING, false);
        var docsInTime = createFakeMessageStateMongoDocuments(10, ENV, Status.DELIVERING, false);

        docsOutOfTime.forEach(doc -> doc.setTimestamp(Date.from(Instant.now().minus(30 + 1 , ChronoUnit.MINUTES))));
        docsInTime.forEach(doc -> doc.setTimestamp(Date.from(Instant.now().minus(30 / 2 , ChronoUnit.MINUTES))));

        var docs = Stream.concat(docsInTime.stream(), docsOutOfTime.stream()).toList();
        return new SliceImpl<>(docs);
    }
}
