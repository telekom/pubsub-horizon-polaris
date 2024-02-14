// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.helper;

import brave.ScopedSpan;
import brave.Span;
import com.fasterxml.jackson.core.JsonProcessingException;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.StatusMessage;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.data.domain.SliceImpl;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static de.telekom.horizon.polaris.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Slf4j
class RepublisherTest {


    @Mock
    KafkaTemplate<String, String> transactionalKafkaTemplate;
    @Mock
    EventWriter eventWriter;
    @Mock
    PartialSubscriptionCache partialSubscriptionCache;
    @Mock
    PolarisConfig polarisConfig;
    @Mock
    HorizonTracer tracer;


    Republisher republisher;

    @BeforeEach
    void prepare() {


        when(tracer.startScopedSpan(any())).thenReturn(mock(ScopedSpan.class));


        republisher = new Republisher(transactionalKafkaTemplate, partialSubscriptionCache, polarisConfig, tracer, eventWriter);

    }

    @ParameterizedTest
    @EnumSource(Status.class)
    @DisplayName("should pick no messages with null coords")
    void shouldPickMessages(Status status) {
        var docs = MockGenerator.createFakeMessageStateMongoDocuments(10, ENV, status, true);
        docs.forEach(doc -> doc.setCoordinates(null));

        var slice = new SliceImpl<>(docs);

        var horizonMessages = republisher.pickMessages(slice);

        verify(transactionalKafkaTemplate, never()).receive(anyString(), anyInt(), anyLong(), any(Duration.class));


        log.info("horizonMessages: {}", horizonMessages);
        assertEquals(10, horizonMessages.size());
        horizonMessages.forEach(horizonMessage -> {
            assertInstanceOf(StatusMessage.class, horizonMessage);
            assertEquals(Status.FAILED, ((StatusMessage)horizonMessage).getStatus());
        });
    }

    @ParameterizedTest
    @EnumSource(Status.class)
    @DisplayName("should pick no messages with not findable coords")
    void shouldPickNoMessagesWithNotFindableCoords(Status status) {
        var docs = MockGenerator.createFakeMessageStateMongoDocuments(10, ENV, status, true);
        var slice = new SliceImpl<>(docs);

        when(transactionalKafkaTemplate.receive(anyString(), anyInt(), anyLong(), any(Duration.class))).thenReturn(null);

        var horizonMessages = republisher.pickMessages(slice);
        boolean allHorizonMessagedAreFailed = horizonMessages.stream().allMatch(identifiableMessage -> {
            if (identifiableMessage instanceof StatusMessage) {
                return Status.FAILED.equals(((StatusMessage) identifiableMessage).getStatus());
            }
            return false;
        });
        log.info("horizonMessages: {}", horizonMessages);
        log.info("allHorizonMessagedAreFailed: {}", allHorizonMessagedAreFailed);
        assertTrue(allHorizonMessagedAreFailed);
    }

    @Test
    @DisplayName("should pick messages and update delivery type and set state")
    void shouldPickNoMessagesThatCanNotBeDeserialized() throws JsonProcessingException {
        final int eventCount = 10;

        var docs = MockGenerator.createFakeMessageStateMongoDocuments(eventCount, ENV, Status.WAITING, false);
        Date initialDocsTimestamp = docs.get(0).getTimestamp();

        var slice = new SliceImpl<>(docs);

        var fakePartialSubscription = new PartialSubscription(ENV, SUBSCRIPTION_ID, PUBLISHER_ID, SUBSCRIBER_ID, CALLBACK_URL, DeliveryType.SERVER_SENT_EVENT, false, false);

        var fakeSubscriptionEventMessage = MockGenerator.createFakeSubscriptionEventMessage(DeliveryType.CALLBACK, true);
        var fakeConsumerRecord = MockGenerator.createFakeConsumerRecord(fakeSubscriptionEventMessage);

        when(partialSubscriptionCache.get(eq(SUBSCRIPTION_ID))).thenReturn(Optional.ofNullable(fakePartialSubscription)); // Subscription has SSE, while consumer record has CALLBACK

        when(transactionalKafkaTemplate.receive(any(), anyInt(), anyLong(), any(Duration.class)))
                .thenReturn(null)
                .thenReturn(fakeConsumerRecord);

        var mockSpan = Mockito.mock(Span.class);
        when(tracer.startSpanFromKafkaHeaders(anyString(), any())).thenReturn(mockSpan);

        var identifiableMessages = republisher.pickMessages(slice);

        log.info("identifiableMessages: {}", identifiableMessages);
        assertEquals(eventCount, identifiableMessages.size());

        var statusMessages = identifiableMessages.stream()
                .filter(identifiableMessage -> identifiableMessage instanceof StatusMessage)
                .map(identifiableMessage -> (StatusMessage) identifiableMessage)
                .toList();
        assertEquals(1, statusMessages.size());
        var failedStatusMessage = statusMessages.getFirst();
        assertNotNull(failedStatusMessage);
        assertEquals(Status.FAILED, failedStatusMessage.getStatus());
        // When FAILED the delivery type keeps the same and does not get updated to the current delivery type
        assertEquals(fakeSubscriptionEventMessage.getDeliveryType(), failedStatusMessage.getDeliveryType());

        var subscriptionEventMessages = identifiableMessages.stream().filter(identifiableMessage -> identifiableMessage instanceof SubscriptionEventMessage)
                .map(identifiableMessage -> (SubscriptionEventMessage) identifiableMessage)
                .toList();
        assertEquals(9, subscriptionEventMessages.size());
        for (var subscriptionEventMessage : subscriptionEventMessages) {
            assertNotNull(subscriptionEventMessage);
            assertEquals(DeliveryType.SERVER_SENT_EVENT, subscriptionEventMessage.getDeliveryType());
            assertEquals(Status.PROCESSED, subscriptionEventMessage.getStatus());
            assertNotEquals(fakeSubscriptionEventMessage.getDeliveryType(), subscriptionEventMessage.getDeliveryType());
        }
    }
}
