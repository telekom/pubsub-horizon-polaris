// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.helper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.model.event.IdentifiableMessage;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.StatusMessage;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import de.telekom.eni.pandora.horizon.mongo.model.MessageStateMongoDocument;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CouldNotPickMessageException;
import de.telekom.horizon.polaris.exception.HorizonPolarisException;
import de.telekom.horizon.polaris.model.PartialSubscription;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.domain.Slice;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Helper class for picking and republishing events from Kafka.
 * <p>
 * This class facilitates the process of picking events from Kafka and republishing them with updated statuses.
 * </p>
 *
 * @since 3.0
 */
@Slf4j
public class Republisher {
    protected final KafkaTemplate<String, String> kafkaTemplate;
    protected final PartialSubscriptionCache partialSubscriptionCache;
    protected final PolarisConfig polarisConfig;
    protected final HorizonTracer tracer;
    protected final EventWriter eventWriter;
    protected static final ObjectMapper objectMapper = new ObjectMapper();

    public Republisher(KafkaTemplate<String, String> kafkaTemplate,
                       PartialSubscriptionCache partialSubscriptionCache,
                       PolarisConfig polarisConfig,
                       HorizonTracer tracer,
                       EventWriter eventWriter) {
        this.kafkaTemplate = kafkaTemplate;
        this.partialSubscriptionCache = partialSubscriptionCache;
        this.polarisConfig = polarisConfig;
        this.tracer = tracer;
        this.eventWriter = eventWriter;
    }

    /**
     * Picks the consumer records from the DB and republishes them.
     * Successfully picked messages and republishes them as {@link SubscriptionEventMessage} with the status {@link Status#PROCESSED}.
     * Unsuccessful picked messages are send as {@link StatusMessage} with the status {@link Status#FAILED}
     *
     * @return The {@link MessageStateMongoDocument MessageStateMongoDocuments} whereof the EventMessage could not get picked from Kafka
     */
    public void pickAndRepublishBatch(Slice<MessageStateMongoDocument> messageStates) {
        log.info("Picking old messages & generating the producer records for {} event/message states", messageStates.getNumberOfElements());
        log.debug("messageStates: {}", messageStates.getContent());
        if(messageStates.getNumberOfElements() <= 0) { return; }

        Map<String, String> uuidToTopic = messageStates.stream().collect(Collectors.toMap(MessageStateMongoDocument::getUuid, MessageStateMongoDocument::getTopic));

        // Pick and republish 5 messages
        log.warn("Pick and republish {} messages", messageStates.getNumberOfElements());
        List<IdentifiableMessage> identifiableMessages = pickMessages(messageStates);

        log.info("Republishing {} identifiableMessages", identifiableMessages.size());
        log.debug("identifiableMessages: {}", identifiableMessages);
        if(!identifiableMessages.isEmpty()) {
            for(var identifiableMessage: identifiableMessages) {
                try {
                    eventWriter.send(uuidToTopic.getOrDefault(identifiableMessage.getUuid(), EventRetentionTime.DEFAULT.getTopic()), identifiableMessage);
                } catch (JsonProcessingException e) {
                    log.error("Could not publish message {}", identifiableMessage);
                }
            }
        }
        log.info("Republished {} identifiableMessages", identifiableMessages.size());
    }

    /**
     * Picks messages from Kafka based on information retrieved from the database and returns a list of {@link IdentifiableMessage}s.
     * <p>
     * For each message state in the provided list, this method attempts to pick the corresponding message from Kafka,
     * resets its status to PROCESSED, and adds it to the list of identifiable messages.
     * </p>
     * <p>
     * If an error occurs during the picking or processing of a message, a new identifiable message with a FAILED status
     * is created, containing information about the error.
     * </p>
     *
     * @param messageStates The message states containing information about the events to be picked and republished.
     * @return The list of {@link IdentifiableMessage identifiable messages} representing the picked and processed events.
     */
    protected List<IdentifiableMessage> pickMessages(Slice<MessageStateMongoDocument> messageStates) {
        List<IdentifiableMessage> identifiableMessages = new ArrayList<>();
        for (var messageState : messageStates) {
            log.debug("messageState: {}", messageState);
            var subscriptionId = messageState.getSubscriptionId();
            var coords = messageState.getCoordinates();
            var topic = Objects.requireNonNullElse(messageState.getEventRetentionTime(), EventRetentionTime.DEFAULT).getTopic();

            IdentifiableMessage identifiableMessage = null;

            if(coords == null) {
                log.warn("Can not pick event from subscription {} with invalid partition/offset from topic {}. Event id {}", subscriptionId , topic ,messageState.getEvent().getId());
                identifiableMessage = new StatusMessage(messageState.getUuid(), messageState.getEvent(), subscriptionId, Status.FAILED, messageState.getDeliveryType()).withThrowable(new CouldNotPickMessageException("Coordinates are null!"));
                identifiableMessages.add(identifiableMessage);
                continue;
            }

            try {
                var oConsumerRecord = pick(topic, coords.partition(), coords.offset());
                if(oConsumerRecord.isEmpty()) {
                    log.warn("Could not pick event {}. Picked consumer record is null!", generateEventLogMessageText(messageState.getEvent().getId(), topic, coords.offset(), coords.partition()));

                    identifiableMessage = new StatusMessage(messageState.getUuid(), messageState.getEvent(), subscriptionId, Status.FAILED, messageState.getDeliveryType()).withThrowable(new CouldNotPickMessageException("Picked consumer record is null!"));
                } else {
                    var subscriptionEventMessage = deserializeSubscriptionEventMessage(oConsumerRecord.get().value());
                    identifiableMessage = resetSubscriptionEventMessage(subscriptionEventMessage, messageState, oConsumerRecord.get());
                }
            } catch (HorizonPolarisException exception) {
                log.error("Could not pick event {}", generateEventLogMessageText(messageState.getEvent().getId(), topic, coords.offset(), coords.partition()), exception);
                identifiableMessage = new StatusMessage(messageState.getUuid(), messageState.getEvent().getId(), Status.FAILED, messageState.getDeliveryType()).withThrowable(exception);
            } catch(JsonProcessingException exception) {
                log.error("Could not deserialize SubscriptionEventMessage for event {}", generateEventLogMessageText(messageState.getEvent().getId(), topic, coords.offset(), coords.partition()), exception);
                identifiableMessage = new StatusMessage(messageState.getUuid(), messageState.getEvent().getId(), Status.FAILED, messageState.getDeliveryType()).withThrowable(exception);
            } catch(Exception exception)  {
                log.error("Unexpected Exception while picking message for event {}", generateEventLogMessageText(messageState.getEvent().getId(), topic, coords.offset(), coords.partition()), exception);
                identifiableMessage = new StatusMessage(messageState.getUuid(), messageState.getEvent().getId(), Status.FAILED, messageState.getDeliveryType()).withThrowable(exception);
            } finally {
                if(identifiableMessage != null) {
                    identifiableMessages.add(identifiableMessage);
                } else {
                    log.error("identifiableMessage for event {} is null, even-though it should not be able to be null. Can not add identifiableMessage to identifiableMessage while picking the SubscribedMessages from Kafka.", generateEventLogMessageText(messageState.getEvent().getId(), topic, coords.offset(), coords.partition()));
                }
            }
        }

        return identifiableMessages;
    }

    /**
     * Resets the SubscriptionEventMessage based on the information retrieved from the Kafka {@link ConsumerRecord} and updates
     * its status to PROCESSED.
     *
     * @param subscriptionEventMessage The SubscriptionEventMessage to be reset.
     * @param messageState             The MessageStateMongoDocument containing information about the event.
     * @param consumerRecord           The ConsumerRecord containing information about the picked event.
     * @return The reset SubscriptionEventMessage.
     */
    private SubscriptionEventMessage resetSubscriptionEventMessage(SubscriptionEventMessage subscriptionEventMessage, MessageStateMongoDocument messageState, ConsumerRecord<String, String> consumerRecord) {
        var republishSpan = tracer.startSpanFromKafkaHeaders("picked message from kafka", consumerRecord.headers());
        republishSpan.finish();

        // Here 5!!! 4 was FAILED 1 was WAITING!!!
        log.warn("Resetting SubscriptionEventMessage with subscriptionId {} and deliveryType {} and status {}", messageState.getSubscriptionId(), messageState.getDeliveryType(), messageState.getStatus());

        // Update delivery type of subscriptionEventMessage to delivery type in subscription cache
        var oPartialSubscription = partialSubscriptionCache.get(subscriptionEventMessage.getSubscriptionId());
        var oDeliveryType = oPartialSubscription.map(PartialSubscription::deliveryType);
        oDeliveryType.ifPresent(
                currDeliveryType -> {
                    subscriptionEventMessage.setDeliveryType(currDeliveryType);
                    messageState.setDeliveryType(currDeliveryType);
                }
        );

        subscriptionEventMessage.setStatus(Status.PROCESSED);
        return subscriptionEventMessage;
    }

    private String generateEventLogMessageText(String eventId, String topic, long offset, int partition) {
        return String.format("with id %s sitting at offset (%d) | partition (%d) in topic %s", eventId, offset, partition, topic);
    }

    private SubscriptionEventMessage deserializeSubscriptionEventMessage(String json) throws JsonProcessingException {
        return objectMapper.readValue(json, SubscriptionEventMessage.class);
    }

    private Optional<ConsumerRecord<String, String>> pick(String topic, int partition, long offset) throws HorizonPolarisException {
        try {
            return Optional.ofNullable(kafkaTemplate.receive(topic, partition, offset, Duration.of(5000, ChronoUnit.MILLIS)));
        } catch (Exception e) {
            throw new CouldNotPickMessageException(e.getMessage(), e);
        }
    }
}
