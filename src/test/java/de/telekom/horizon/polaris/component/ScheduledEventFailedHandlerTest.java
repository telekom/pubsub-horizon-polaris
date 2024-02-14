package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.model.db.StateError;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.horizon.polaris.exception.CallbackException;
import de.telekom.horizon.polaris.exception.CouldNotDetermineWorkingSetException;
import de.telekom.horizon.polaris.kubernetes.PodResourceEventHandler;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Optional;

import static de.telekom.horizon.polaris.TestConstants.*;
import static de.telekom.horizon.polaris.util.MockGenerator.createFakeMessageStateMongoDocuments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Slf4j
class ScheduledEventFailedHandlerTest {

    private ThreadPoolService threadPoolService;

    PodResourceEventHandler podResourceEventHandler;

    public ScheduledEventFailedHandler scheduledEventFailedHandler;


    @BeforeEach
    public void prepare() throws CouldNotDetermineWorkingSetException {

        threadPoolService = MockGenerator.mockThreadPoolService();

        doReturn(true).when(MockGenerator.polarisService).areResourcesFullySynced();
//        when(MockGenerator.podService.getAllPods()).thenReturn(List.of(POD_NAME));

        var fakePartialSubscription = new PartialSubscription(ENV, SUBSCRIPTION_ID, PUBLISHER_ID, SUBSCRIBER_ID, CALLBACK_URL, DeliveryType.CALLBACK, false, false);

        when(MockGenerator.partialSubscriptionCache.get(anyString())).thenReturn(Optional.ofNullable(fakePartialSubscription));

        when(MockGenerator.polarisConfig.getPodName()).thenReturn(POD_NAME);
        when(MockGenerator.polarisConfig.getPollingBatchSize()).thenReturn(10);
        when(threadPoolService.getPolarisConfig()).thenReturn(MockGenerator.polarisConfig);

        when(threadPoolService.getPartialSubscriptionCache()).thenReturn(MockGenerator.partialSubscriptionCache);

        var slice = createFakeSlice();
        when(MockGenerator.messageStateMongoRepo.findStatusFailedWithCallbackExceptionAsc(any())).thenReturn(slice);

        when(threadPoolService.getMessageStateMongoRepo()).thenReturn(MockGenerator.messageStateMongoRepo);


        var fakePod = MockGenerator.createFakePod(POD_NAME);
        MockGenerator.podResourceEventHandler.onAdd(fakePod);

        scheduledEventFailedHandler =  spy(new ScheduledEventFailedHandler(threadPoolService, MockGenerator.polarisService));
    }

    @Test
    @DisplayName("should do nothing if resources are not fully synced")
    void shouldDoNothingIfResourcesAreNotFullySynced() throws CouldNotDetermineWorkingSetException {
        doReturn(false).when(MockGenerator.polarisService).areResourcesFullySynced();

        scheduledEventFailedHandler.run();

        verify(MockGenerator.polarisConfig, never()).getPollingBatchSize();
        verify(MockGenerator.messageStateMongoRepo, never()).findStatusFailedWithCallbackExceptionAsc(isA(Pageable.class));
        verify(MockGenerator.threadPoolService, never()).startRepublishTask(any());
    }

    @Test
    @DisplayName("should call processMessagesStates if resources are fully synced")
    void shouldWorkIfResourcesAreFullySynced() throws CouldNotDetermineWorkingSetException {

        scheduledEventFailedHandler.run();

        verify(MockGenerator.polarisConfig, times(1)).getPollingBatchSize();
        verify(MockGenerator.messageStateMongoRepo, atLeastOnce()).findStatusFailedWithCallbackExceptionAsc(isA(Pageable.class));
        verify(MockGenerator.threadPoolService, atLeastOnce()).startRepublishTask(any());
    }

    @Test
    @DisplayName("should start RepublishTask")
    void shouldStartSubscriptionComparisonTask() throws CouldNotDetermineWorkingSetException {
        scheduledEventFailedHandler.run();

        verify(threadPoolService, times(1)).startRepublishTask(notNull());
    }

    @Test
    @DisplayName("should not start SubscriptionComparisonTask when subscriptionId was not found")
    void shouldNotStartSubscriptionComparisonTask() throws CouldNotDetermineWorkingSetException {

        when(MockGenerator.partialSubscriptionCache.get(anyString())).thenReturn(Optional.empty());

        scheduledEventFailedHandler.run();

        verify(threadPoolService, never()).startSubscriptionComparisonTask(notNull(), notNull());
    }

    Slice<MessageStateMongoDocument> createFakeSlice() {
        var docs = createFakeMessageStateMongoDocuments(10, ENV, Status.FAILED, true);
        var callbackStateError = StateError.fromException(new CallbackException("bla"));
        docs.forEach(doc -> doc.setError(callbackStateError));

        return new SliceImpl<>(docs);
    }
}
