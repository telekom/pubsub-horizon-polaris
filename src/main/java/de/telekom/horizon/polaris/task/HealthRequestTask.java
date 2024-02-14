package de.telekom.horizon.polaris.task;

import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.component.HealthCheckRestClient;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CallbackException;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.StatusLine;
import org.springframework.http.HttpMethod;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Task to make a HEAD or GET request to a given callback URL and environment, updating health check data and
 * initiating further tasks based on the result.
 *
 * @author Tim Pütz
 * @since 3.0
 */
@Slf4j
public class HealthRequestTask implements Callable<Boolean> {
    private final String callbackUrl;

    private final String publisherId;
    private final String subscriberId;
    private final String environment;
    private final HttpMethod httpMethod;
    private final HealthCheckRestClient restClient;
    private final HealthCheckCache healthCheckCache;
    private final CircuitBreakerCacheService circuitBreakerCache;
    private final ThreadPoolService threadPoolService;
    private final PolarisConfig polarisConfig;


    public HealthRequestTask(String callbackUrl, String publisherId, String subscriberId, String environment, HttpMethod httpMethod, ThreadPoolService threadPoolService) {
        this.callbackUrl = callbackUrl;
        this.publisherId = publisherId;
        this.subscriberId = subscriberId;
        this.environment = environment;
        this.httpMethod = httpMethod;
        this.restClient = threadPoolService.getRestClient();
        this.healthCheckCache = threadPoolService.getHealthCheckCache();
        this.circuitBreakerCache = threadPoolService.getCircuitBreakerCacheService();
        this.polarisConfig = threadPoolService.getPolarisConfig();
        this.threadPoolService = threadPoolService;
    }

    /**
     * Calculates a cooldown for a given republishCount.
     * <br><br>
     * Equation: 2 ^ republishCount
     * <br>
     * Max 60 minutes
     */
    public static Duration calculateCooldown(int republishCount) {
        if (republishCount == 0) {
            return Duration.ofMinutes(0);
        }

        return Duration.ofMinutes((int) Math.min(Math.pow(2, republishCount), 60));
    }


    @Override
    public Boolean call() {
        boolean wasSuccessful = false;
        try {
            healthCheckCache.update(callbackUrl, httpMethod, true); // should be true already, just to make sure
            var oReturnStatusLine = executeHealthCheckRequest(callbackUrl, publisherId, subscriberId, environment, httpMethod);

            if (oReturnStatusLine.isEmpty()) {
                log.warn("Could not execute health check request, returned statusLine is empty!");
            } else {
                var returnStatusLine = oReturnStatusLine.get();
                var statusCode = returnStatusLine.getStatusCode();
                updateHealthCheckInCaches(statusCode, returnStatusLine.getReasonPhrase());

                wasSuccessful = polarisConfig.getSuccessfulStatusCodes().contains(statusCode);
            }
        } catch (Exception exception) {
            log.error("Unexpected error while executing health check request or updating health check in caches.", exception);
        }

        return wasSuccessful;
    }

    /**
     * Updates health check data in caches based on the provided status code and reason phrase.
     *
     * @param statusCode   The HTTP status code from the health check response.
     * @param reasonPhrase The reason phrase from the health check response.
     */
    private void updateHealthCheckInCaches(int statusCode, String reasonPhrase) {
        // Get all subscriptions for callbackUrl from callbackUrl2Subscriptions map
        // Update last health check in CBMessage for each subscription & callbackUrl2Subscriptions map
        var subscriptionIds = healthCheckCache.getSubscriptionIds(callbackUrl, httpMethod);
        var healthCheckData = healthCheckCache.update(callbackUrl, httpMethod, statusCode, reasonPhrase);

        var circuitBreakerMessages = subscriptionIds.stream()
                .map(circuitBreakerCache::getCircuitBreakerMessage)
                .filter(Optional::isPresent)
                .map(Optional::get).toList();

        for (var cbMessage : circuitBreakerMessages) {
            cbMessage.setLastHealthCheck(healthCheckData.getLastHealthCheckOrNull());
            circuitBreakerCache.updateCircuitBreakerMessage(cbMessage);
        }
    }

    /**
     * Executes the health check request and returns an {@link Optional} containing the {@link StatusLine} of the response.
     * Returns an empty {@link Optional} if the health check request encounters an exception.
     *
     * @param callbackUrl   The callback URL to be health-checked.
     * @param publisherId   The publisher ID associated with the health check request.
     * @param subscriberId  The subscriber ID associated with the health check request.
     * @param environment   The environment associated with the health check request.
     * @param httpMethod    The HTTP method (HEAD or GET) for the health check request.
     * @return An Optional containing the StatusLine of the health check response or an empty Optional if an exception occurs.
     */
    protected Optional<StatusLine> executeHealthCheckRequest(String callbackUrl, String publisherId, String subscriberId, String environment, HttpMethod httpMethod) {
        try {
            StatusLine returnStatusLine;
            if (httpMethod.equals(HttpMethod.HEAD)) {
                returnStatusLine = restClient.head(callbackUrl, publisherId, subscriberId, environment);
            } else if (httpMethod.equals(HttpMethod.GET)) {
                returnStatusLine = restClient.get(callbackUrl, publisherId, subscriberId, environment);
            } else {
                throw new IllegalArgumentException("HttpMethods needs to be HEAD or GET");
            }

            log.info("Health check request ({}) for callback url '{}' returned code '{}'", httpMethod, callbackUrl, returnStatusLine.getStatusCode());
            return Optional.ofNullable(returnStatusLine);
        } catch (CallbackException | NullPointerException e) {
            log.error(e.getMessage(), e);
        }
        return Optional.empty();
    }

}
