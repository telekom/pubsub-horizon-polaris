// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component.rest;

import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.model.CloseCircuitBreakersRequest;
import de.telekom.horizon.polaris.model.CloseCircuitBreakersResponse;
import de.telekom.horizon.polaris.model.PartialSubscription;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.task.RepublishPartialSubscriptionsTask;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.springframework.http.HttpStatus.*;

@AllArgsConstructor
@RestController()
@RequestMapping("circuit-breakers")
@Slf4j
public class CircuitBreakerController {
    private final CircuitBreakerCacheService circuitBreakerCacheService;
    private final ThreadPoolService threadPoolService;
    private final PartialSubscriptionCache partialSubscriptionCache;

    /**
     * Retrieves the Circuit Breaker message for the specified subscription ID.
     *
     * @param subscriptionId The subscription ID.
     * @return The Circuit Breaker message.
     * @throws ResponseStatusException If the resource is not found (HTTP 404).
     */
    @GetMapping(value = "/{subscriptionId}", produces = "application/json")
    public CircuitBreakerMessage getCircuitBreakerMessage(@PathVariable() String subscriptionId) {
        var oCircuitBreakerMessage = circuitBreakerCacheService.getCircuitBreakerMessage(subscriptionId);
        if(oCircuitBreakerMessage.isEmpty()) { throw new ResponseStatusException(NOT_FOUND, "Unable to find resource"); } // 404

        return oCircuitBreakerMessage.get();
    }

    @GetMapping(produces = "application/json")
    public List<CircuitBreakerMessage> getCircuitBreakerMessages() {
        return circuitBreakerCacheService.getCircuitBreakerMessages();
    }

    @GetMapping(params = {"page", "size"}, produces = "application/json")
    public List<CircuitBreakerMessage> getCircuitBreakerMessages(@RequestParam() int page, @RequestParam() int size) {
        return circuitBreakerCacheService.getCircuitBreakerMessages(page, size);
    }

    @GetMapping(params = {"page", "size", "status"}, produces = "application/json")
    public List<CircuitBreakerMessage> getCircuitBreakerMessages(@RequestParam() int page, @RequestParam() int size, @RequestParam() CircuitBreakerStatus status) {
        return circuitBreakerCacheService.getCircuitBreakerMessages(page, size, status);
    }

    /**
     * Closes Circuit Breakers based on the provided request.
     * <p>
     * This method processes the {@link CloseCircuitBreakersRequest} to close Circuit Breakers for the specified
     * subscription IDs. It starts {@link RepublishPartialSubscriptionsTask} for every found subscription ID.
     * </p>
     *
     * @param closeCircuitBreakersRequest The request containing subscription IDs to close Circuit Breakers.
     * @return The response indicating the success of closing Circuit Breakers.
     * @throws ResponseStatusException If the request body is invalid (HTTP 400).
     */
    @DeleteMapping(produces = "application/json")
    public CloseCircuitBreakersResponse closeCircuitBreakers(@RequestBody CloseCircuitBreakersRequest closeCircuitBreakersRequest) {
        if(Objects.isNull(closeCircuitBreakersRequest) || Objects.isNull(closeCircuitBreakersRequest.subscriptionIds())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Provide a valid request body", null);
        }

        var closeCircuitBreakersResponse = new CloseCircuitBreakersResponse(new ArrayList<>());
        var partialSubscriptions = new ArrayList<PartialSubscription>();

        for (var subscriptionId: closeCircuitBreakersRequest.subscriptionIds()) {
            var oCircuitBreakerMessage = circuitBreakerCacheService.getCircuitBreakerMessage(subscriptionId);
            if(oCircuitBreakerMessage.isEmpty()) {
                throw new ResponseStatusException(NOT_FOUND, "Unable to find circuit breaker message");
            }

            if(CircuitBreakerStatus.REPUBLISHING.equals(oCircuitBreakerMessage.get().getStatus())) {
                throw new ResponseStatusException(CONFLICT, "Circuit breaker is already republishing");
            }

            if(CircuitBreakerStatus.OPEN.equals(oCircuitBreakerMessage.get().getStatus())) {
                throw new ResponseStatusException(TOO_EARLY, "Circuit breaker is in OPEN status and will be processed automatically");
            }

            var oPartialSubscription = partialSubscriptionCache.get(subscriptionId);
            // If not found in SubscriptionCache, build from CircuitBreakerMessage
            if(oPartialSubscription.isEmpty()) {
                closeCircuitBreakersResponse.subscriberIdsNotFoundInSubscriptionCache().add(subscriptionId);
                oPartialSubscription = Optional.of(PartialSubscription.fromCircuitBreakerMessage(oCircuitBreakerMessage.get()));
            }

            partialSubscriptions.add(oPartialSubscription.get());
        }

        try {
            threadPoolService.startRepublishSubscriptionIdsTask(partialSubscriptions).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to execute and wait for republish task", e);
            throw new ResponseStatusException(INTERNAL_SERVER_ERROR, "Failed to start republish task");
        }

        return closeCircuitBreakersResponse;
    }
}
