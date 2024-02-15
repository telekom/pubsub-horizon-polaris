// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.task;

import de.telekom.eni.pandora.horizon.model.db.State;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.horizon.polaris.helper.Republisher;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.*;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.List;

import static de.telekom.horizon.polaris.TestConstants.ENV;
import static org.mockito.Mockito.*;

/**
 * Tests the RepublishTask and its protected methods.
 * <br><br>
 * <b>Important</b>: When {@link Republisher#pickAndRepublishBatch} gets called,
 * it throws a catched {@link NullPointerException } that you will find in the log.
 * The reason is that the used services and methods are only mocked and return null values which
 * lead to a {@link NullPointerException }. But we only care that the method is getting called, not that it works.
 *
 * @author Tim Pütz
 * @since 3.0
 */
@ExtendWith(SpringExtension.class)
@Slf4j
class RepublishingTaskTest {
    ThreadPoolService threadPoolService;

    RepublishingTask republishingTask;

    Slice<MessageStateMongoDocument> fakeMessageStates;
    List<String> fakeMessageStatesSubscriptionIds;

    @BeforeEach
    void prepare() {
        threadPoolService = MockGenerator.mockThreadPoolService();

        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "timestamp"));

        var fakeDocs = MockGenerator.createFakeMessageStateMongoDocuments(10, ENV, Status.DELIVERING, true);
        fakeMessageStates = new SliceImpl<>(fakeDocs, pageable, true);

        fakeMessageStatesSubscriptionIds = fakeMessageStates.map(State::getSubscriptionId).getContent();
    }

    @Test
    @DisplayName("should set CircuitBreakers to Republishing")
    void shouldSetCircuitBreakersToRepublishing() {
        var fakeCircuitBreakerMessages = MockGenerator.createFakeCircuitBreakerMessages(5, true);

        // Make 10 messages findable in cache
        for(int i = 0; i < fakeCircuitBreakerMessages.size(); i++) {
            fakeMessageStates.getContent().get(i).setSubscriptionId(fakeCircuitBreakerMessages.get(i).getSubscriptionId());
        }

        fakeMessageStatesSubscriptionIds = fakeMessageStates.map(State::getSubscriptionId).getContent();

        // return correct circuit breaker message optional from fake circuit breaker messages
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessage(anyString()))
                .then( invocationOnMock -> fakeCircuitBreakerMessages.stream().filter(m -> m.getSubscriptionId().equals(invocationOnMock.getArgument(0))).findFirst());

        republishingTask = spy(new RepublishingTask(threadPoolService));

        republishingTask.setCircuitBreakersToRepublishing( fakeMessageStatesSubscriptionIds );

        verify(republishingTask, times(fakeMessageStatesSubscriptionIds.size())).setCircuitBreakerToRepublishing( argThat(fakeMessageStatesSubscriptionIds::contains) );
        verify(MockGenerator.circuitBreakerCache, times(fakeMessageStatesSubscriptionIds.size())).getCircuitBreakerMessage( argThat(fakeMessageStatesSubscriptionIds::contains) );

        // only update when circuit breaker message found
        var cbSubscriptionIds = fakeMessageStates.map(State::getSubscriptionId).getContent();
        verify(MockGenerator.circuitBreakerCache, times(fakeCircuitBreakerMessages.size())).updateCircuitBreakerStatus( argThat(cbSubscriptionIds::contains), eq(CircuitBreakerStatus.REPUBLISHING) );
    }


    @Test
    @DisplayName("should call Pick and RepublishBatch")
    void shouldPickAndRepublishMessages() {

        var fakeDocs = MockGenerator.createFakeMessageStateMongoDocuments(10, ENV, Status.DELIVERING, true);
        Slice<MessageStateMongoDocument> messageStatesToRepublish = new SliceImpl<>(fakeDocs);

        republishingTask = spy(new RepublishingTask(fakeMessageStates, threadPoolService));
        republishingTask.republisher = spy(republishingTask.republisher);

        republishingTask.run();

        verify(republishingTask.republisher, times(1)).pickAndRepublishBatch(eq(fakeMessageStates));
    }

    @Test
    @DisplayName("should query Db, PickStates and RepublishMessages")
    void shouldQueryDbPickStatesAndRepublishMessages() {
        when(MockGenerator.messageStateMongoRepo.findByStatusInPlusCallbackUrlNotFoundExceptionAsc(anyList(), eq(fakeMessageStatesSubscriptionIds), any(), any()))
                .thenReturn( fakeMessageStates ).thenReturn( new SliceImpl<>(new ArrayList<>()) );

        republishingTask = spy(new RepublishingTask(threadPoolService));
        republishingTask.republisher = spy(republishingTask.republisher);

        republishingTask.queryDbPickStatesAndRepublishMessages(fakeMessageStatesSubscriptionIds);

        var argumentCaptor = ArgumentCaptor.forClass(Slice.class);

        verify(republishingTask.republisher, times(2)).pickAndRepublishBatch(argumentCaptor.capture());

        List<Slice> capturedSlices = argumentCaptor.getAllValues();

        Slice<MessageStateMongoDocument> firstCapturedSlice = capturedSlices.get(0);
        Slice<MessageStateMongoDocument> secondCapturedSlice = capturedSlices.get(1);

        Assertions.assertEquals( fakeMessageStates, firstCapturedSlice);
        Assertions.assertEquals(0, secondCapturedSlice.getNumberOfElements());
    }
}
