package de.telekom.horizon.polaris.service;

import com.hazelcast.query.PagingPredicate;
import de.telekom.eni.pandora.horizon.cache.service.CacheService;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

import static com.hazelcast.query.Predicates.*;

/**
 * Service for interacting with the circuit breaker {@link CacheService}.
 */
@Slf4j
@Component
@AllArgsConstructor
public class CircuitBreakerCacheService {
    private final CacheService circuitBreakerCache;

    /**
     * Gets all {@link CircuitBreakerMessage}s from the cache.
     *
     * @return A list of all {@link CircuitBreakerMessage}s.
     */
    public List<CircuitBreakerMessage> getCircuitBreakerMessages() {
        return this.circuitBreakerCache.getValues();
    }

    /**
     * Gets a {@link CircuitBreakerMessage} by subscription ID.
     *
     * @param subscriptionId The subscription ID.
     * @return An Optional containing the {@link CircuitBreakerMessage}, or empty if not found.
     */
    public Optional<CircuitBreakerMessage> getCircuitBreakerMessage(String subscriptionId) {
        return circuitBreakerCache.get(subscriptionId);
    }

    /**
     * Gets all open {@link CircuitBreakerMessage}s with paging.
     *
     * @param page The page number.
     * @param size The page size.
     * @return A list of open {@link CircuitBreakerMessage}s.
     */
    public List<CircuitBreakerMessage> getOpenCircuitBreakerMessages(int page, int size) {
        return getCircuitBreakerMessages(page, size, CircuitBreakerStatus.OPEN);
    }

    /**
     * Gets {@link CircuitBreakerMessage}s with paging.
     *
     * @param page The page number.
     * @param size The page size.
     * @return A list of {@link CircuitBreakerMessage}s.
     */
    public List<CircuitBreakerMessage> getCircuitBreakerMessages(int page, int size) {
        log.debug("getCircuitBreakerMessages: page: {}, size: {}", page, size);
        PagingPredicate<String, CircuitBreakerMessage> pagingPredicate = pagingPredicate(size);
        pagingPredicate.setPage(page);

        return circuitBreakerCache.getWithQuery(pagingPredicate);
    }

    /**
     * Gets {@link CircuitBreakerMessage}s with paging and a specific status.
     *
     * @param page   The page number.
     * @param size   The page size.
     * @param status The circuit breaker status.
     * @return A list of {@link CircuitBreakerMessage}s with the specified status.
     */
    public List<CircuitBreakerMessage> getCircuitBreakerMessages(int page, int size, CircuitBreakerStatus status) {
        log.debug("getCircuitBreakerMessages: page: {}, size: {}, status: {}", page, size, status);
        var predicate = equal("status", status);
        PagingPredicate<String, CircuitBreakerMessage> pagingPredicate = pagingPredicate(predicate, size);
        pagingPredicate.setPage(page);

        return circuitBreakerCache.getWithQuery(pagingPredicate);
    }


    /**
     * Gets {@link CircuitBreakerMessage}s with paging and a specific assigned pod.
     *
     * @param page        The page number.
     * @param size        The page size.
     * @param assignedPod The assigned pod.
     * @return A list of {@link CircuitBreakerMessage}s with the specified assigned pod.
     */
    public List<CircuitBreakerMessage> getCircuitBreakerMessages(int page, int size, String assignedPod) {
        log.debug("getCircuitBreakerMessages: page: {}, size: {}, assignedPod: {}", page, size, assignedPod);

        // maybe add equal("type", "circuit-breaker") again
        var predicate = equal("assignedPodId", assignedPod);
        PagingPredicate<String, CircuitBreakerMessage> pagingPredicate = pagingPredicate(predicate, size);
        pagingPredicate.setPage(page);

        return circuitBreakerCache.getWithQuery(pagingPredicate);
    }

    /**
     * Gets {@link CircuitBreakerMessage}s with paging, a specific status, and a specific assigned pod.
     *
     * @param page        The page number.
     * @param size        The page size.
     * @param status      The circuit breaker status.
     * @param assignedPod The assigned pod.
     * @return A list of {@link CircuitBreakerMessage}s with the specified status and assigned pod.
     */
    public List<CircuitBreakerMessage> getCircuitBreakerMessages(int page, int size, CircuitBreakerStatus status, String assignedPod) {
        log.debug("getCircuitBreakerMessages: page: {}, size: {}, status: {}, assignedPod: {}", page, size, status, assignedPod);

        // maybe add equal("type", "circuit-breaker") again
        var predicate = and(equal("status", status.getValue()), equal("assignedPodId", assignedPod));
        PagingPredicate<String, CircuitBreakerMessage> pagingPredicate = pagingPredicate(predicate, size);
        pagingPredicate.setPage(page);

        return circuitBreakerCache.getWithQuery(pagingPredicate);
    }



    /**
     * Closes the circuit breaker for the specified subscription ID.
     *
     * @param subscriptionId The subscription ID.
     */
    public void closeCircuitBreaker(String subscriptionId) {
        circuitBreakerCache.remove(subscriptionId);
    }

    /**
     * Closes circuit breakers if they are in the REPUBLISHING status for the specified subscription IDs.
     *
     * @param subscriptionIds The list of subscription IDs.
     */
    public void closeCircuitBreakersIfRepublishing(List<String> subscriptionIds) {
        for (var subscriptionId: subscriptionIds) {
            var result = circuitBreakerCache.get(subscriptionId);
            if (result.isEmpty())
                continue;
            var circuitBreakerMessage = (CircuitBreakerMessage) result.get();
            if (!CircuitBreakerStatus.REPUBLISHING.equals(circuitBreakerMessage.getStatus()))
                continue;
            closeCircuitBreaker(subscriptionId);
        }
    }


    /**
     * Updates the {@link CircuitBreakerMessage} in the cache.
     *
     * @param circuitBreakerMessage The updated {@link CircuitBreakerMessage}.
     */
    public void updateCircuitBreakerMessage(CircuitBreakerMessage circuitBreakerMessage) {
        circuitBreakerCache.update(circuitBreakerMessage.getSubscriptionId(), circuitBreakerMessage);
    }

    /**
     * Updates the status of the circuit breaker for the specified subscription ID.
     *
     * @param subscriptionId The subscription ID.
     * @param status         The new circuit breaker status.
     */
    public void updateCircuitBreakerStatus(String subscriptionId, CircuitBreakerStatus status) {
        var result = circuitBreakerCache.get(subscriptionId);
        if (result.isPresent()) {
            var circuitBreakerMessage = (CircuitBreakerMessage) result.get();
            circuitBreakerMessage.setStatus(status);
            circuitBreakerCache.update(subscriptionId, circuitBreakerMessage);
        }
    }
}