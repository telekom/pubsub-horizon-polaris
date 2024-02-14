package de.telekom.horizon.polaris.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static de.telekom.horizon.polaris.TestConstants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 *
 * @author Tim Pütz
 * @since 3.0
 */
@ExtendWith(SpringExtension.class)
@Slf4j
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class HandleSuccessfulHealthRequestTaskTest {

    ThreadPoolService threadPoolService;

    HandleSuccessfulHealthRequestTask handleSuccessfulHealthRequestTask;

    @BeforeEach
    void prepare() {
        threadPoolService = MockGenerator.mockThreadPoolService();

        var fakeCircuitBreakerMessage = MockGenerator.createFakeCircuitBreakerMessages(1).get(0);
        var fakeCircuitBreakerMessageRepublishing = SerializationUtils.clone(fakeCircuitBreakerMessage);
        fakeCircuitBreakerMessageRepublishing.setStatus(CircuitBreakerStatus.REPUBLISHING);
        when(MockGenerator.circuitBreakerCache.getCircuitBreakerMessage( eq(SUBSCRIPTION_ID) ))
                .thenReturn(Optional.ofNullable(fakeCircuitBreakerMessage))
                .thenReturn(Optional.ofNullable(fakeCircuitBreakerMessageRepublishing));

        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "timestamp"));
        var fakeDocs = MockGenerator.createFakeMessageStateMongoDocuments(10, ENV, Status.WAITING,false);
        var fakeMessageStates = new SliceImpl<>(fakeDocs, pageable, true);

        // findByStatusInPlusCallbackUrlNotFoundException ( SSE )
        when(MockGenerator.messageStateMongoRepo.findByStatusInPlusCallbackUrlNotFoundExceptionAsc(anyList(), eq(List.of(SUBSCRIPTION_ID)), any(), any()))
                .thenReturn( fakeMessageStates ).thenReturn( new SliceImpl<>(new ArrayList<>()) );

        // findByStatusInAndDeliveryTypeAndSubscriptionIds ( CALLBACK )
        when(MockGenerator.messageStateMongoRepo.findByStatusInAndDeliveryTypeAndSubscriptionIdsAsc(anyList(), any(DeliveryType.class), anyList(), any()))
                .thenReturn(fakeMessageStates).thenReturn( new SliceImpl<>(new ArrayList<>()) );
    }

    @Test
    void should_set_isThreadOpen_to_false_and_return_when_no_entry_in_HealthCache() {
        String callbackUrl = CALLBACK_URL;
        HttpMethod httpMethod = HttpMethod.HEAD;

        when(MockGenerator.healthCheckCache.get( eq(callbackUrl), eq(httpMethod) )).thenReturn(Optional.empty());

        handleSuccessfulHealthRequestTask = new HandleSuccessfulHealthRequestTask(callbackUrl, httpMethod, threadPoolService);
        handleSuccessfulHealthRequestTask.run();

        verify(MockGenerator.healthCheckCache, times(1)).update( eq(callbackUrl), eq(httpMethod), eq(false) );
        verify(MockGenerator.circuitBreakerCache, never()).updateCircuitBreakerStatus(anyString(), any(CircuitBreakerStatus.class));
    }

    @Test
    void should_work_when_entry_in_HealthCache() throws JsonProcessingException {
        String callbackUrl = CALLBACK_URL;
        HttpMethod httpMethod = HttpMethod.HEAD;

        when(MockGenerator.kafkaTemplate.receive(any(), anyInt(), anyLong(), any(Duration.class))).thenReturn(MockGenerator.createFakeConsumerRecord( DeliveryType.CALLBACK ));
        MockGenerator.healthCheckCache.add( callbackUrl, httpMethod, SUBSCRIPTION_ID );

        handleSuccessfulHealthRequestTask = new HandleSuccessfulHealthRequestTask(callbackUrl, httpMethod, threadPoolService);
        handleSuccessfulHealthRequestTask.run();

        verify(MockGenerator.circuitBreakerCache, times(1)).updateCircuitBreakerStatus(eq(SUBSCRIPTION_ID), eq(CircuitBreakerStatus.REPUBLISHING));
        verify(MockGenerator.circuitBreakerCache, times(1)).closeCircuitBreakersIfRepublishing(eq(List.of(SUBSCRIPTION_ID)));

        verify(MockGenerator.healthCheckCache, times(1)).clearBeforeRepublishing( eq(callbackUrl), eq(httpMethod));
    }

}
