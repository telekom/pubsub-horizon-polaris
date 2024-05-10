// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.model;

import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;

/**
 * Represents a partial subscription extracted from a Kubernetes {@link SubscriptionResource}.
 * This partial subscription contains essential information about the subscription
 * such as environment, subscription ID, publisher ID, subscriber ID, callback URL,
 * delivery type, and configuration flags for HTTP method and circuit breaker opt-out.
 * <p>
 * The class provides factory methods to create instances from CircuitBreakerMessage or SubscriptionResource.
 * </p>
 *
 * @param environment            The environment of the subscription.
 * @param subscriptionId         The unique identifier for the subscription.
 * @param publisherId            The unique identifier for the publisher.
 * @param subscriberId           The unique identifier for the subscriber.
 * @param callbackUrl            The callback URL for the subscription.
 * @param deliveryType           The delivery type for the subscription.
 * @param isGetMethodInsteadOfHead A flag indicating whether the subscription enforces the use of HTTP GET method for health checks.
 * @param isCircuitBreakerOptOut A flag indicating whether the subscription opts out of the circuit breaker mechanism.
 */
public record PartialSubscription(String environment, String subscriptionId, String publisherId, String subscriberId, String callbackUrl, DeliveryType deliveryType, boolean isGetMethodInsteadOfHead, boolean isCircuitBreakerOptOut) {
    @Override
    public String toString() {
        return String.format("PartialSubscription { environment=%s, subscriptionId=%s, publisherId=%s, subscriberId=%s, callbackUrl=%s, deliveryType=%s, isGetMethodInsteadOfHead=%b, isCircuitBreakerOptOut=%b }", environment, subscriptionId, publisherId, subscriberId, callbackUrl, deliveryType, isGetMethodInsteadOfHead, isCircuitBreakerOptOut);
    }

    /**
     * Creates a PartialSubscription instance from a CircuitBreakerMessage.
     *
     * @param circuitBreakerMessage The CircuitBreakerMessage containing information about the subscription.
     * @return A PartialSubscription instance.
     */
    public static PartialSubscription fromCircuitBreakerMessage(CircuitBreakerMessage circuitBreakerMessage) {
        String environment = circuitBreakerMessage.getEnvironment();
        String subscriptionId = circuitBreakerMessage.getSubscriptionId();
        String publisherId = "";
        String subscriberId = circuitBreakerMessage.getSubscriberId();
        String callbackUrl = circuitBreakerMessage.getCallbackUrl();
        DeliveryType deliveryType = DeliveryType.CALLBACK;
        boolean isGetMethodInsteadOfHead = false;
        boolean isCircuitBreakerOptOut = false;

        return new PartialSubscription(environment, subscriptionId, publisherId, subscriberId, callbackUrl, deliveryType, isGetMethodInsteadOfHead, isCircuitBreakerOptOut);
    }

    /**
     * Creates a PartialSubscription instance from a SubscriptionResource.
     *
     * @param subscriptionResource The SubscriptionResource containing information about the subscription.
     * @return A PartialSubscription instance.
     */
    public static PartialSubscription fromSubscriptionResource(SubscriptionResource subscriptionResource) {
        var subscription = subscriptionResource.getSpec().getSubscription();
        var isGetRequest = subscription.isEnforceGetHttpRequestMethodForHealthCheck();
        var isCircuitBreakerOptOut = subscription.isCircuitBreakerOptOut();
        String environment = subscriptionResource.getSpec().getEnvironment();
        String publisherId = subscription.getPublisherId();
        String subscriptionId = subscription.getSubscriptionId();
        String subscriberId = subscription.getSubscriberId();
        String callback = subscription.getCallback();

        DeliveryType deliveryType = DeliveryType.fromString(subscription.getDeliveryType());

        return new PartialSubscription(environment, subscriptionId, publisherId, subscriberId, callback, deliveryType, isGetRequest, isCircuitBreakerOptOut);
    }
}
