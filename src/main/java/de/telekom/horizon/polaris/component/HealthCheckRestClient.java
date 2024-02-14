package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.auth.OAuth2TokenCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CallbackException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Responsible for performing health checks on callback URLs using HTTP HEAD or GET requests.
 * This component utilizes an OAuth2 token for authentication and regularly triggers token retrieval to ensure up-to-date access tokens.
 * <p>
 * The health check requests are sent with appropriate headers, including the OAuth2 token, publisher ID, and subscriber ID.
 * </p>
 * <p>
 * This component is also scheduled to trigger token retrieval every 4 hours, ensuring that access tokens remain valid.
 * </p>
 *
 */
@Component
@Slf4j
public class HealthCheckRestClient {
    public static final String DEFAULT_REALM = "default";
    private final OAuth2TokenCache oAuth2TokenCache;
    private final CloseableHttpClient httpClient;
    private final PolarisConfig polarisConfig;

    @Autowired
    public HealthCheckRestClient(OAuth2TokenCache oAuth2TokenCache, CloseableHttpClient httpClient, PolarisConfig polarisConfig) {
        this.oAuth2TokenCache = oAuth2TokenCache;
        this.polarisConfig = polarisConfig;
        this.oAuth2TokenCache.retrieveAllAccessTokens();

        this.httpClient = httpClient;
    }

    /**
     * Performs an HTTP HEAD request to the specified callback URL for a health check.
     *
     * @param callbackUrl   The URL to perform the health check.
     * @param publisherId   The publisher ID to include in the request header.
     * @param subscriberId  The subscriber ID to include in the request header.
     * @param environment   The environment or realm for which the request is made.
     * @return The HTTP response status line indicating the result of the health check.
     * @throws CallbackException If an error occurs during the health check request.
     */
    public StatusLine head(String callbackUrl, String publisherId, String subscriberId, String environment) throws CallbackException {
        return doRequest(new HttpHead(callbackUrl), publisherId, subscriberId, environment);
    }

    /**
     * Performs an HTTP GET request to the specified callback URL for a health check.
     *
     * @param callbackUrl   The URL to perform the health check.
     * @param publisherId   The publisher ID to include in the request header.
     * @param subscriberId  The subscriber ID to include in the request header.
     * @param environment   The environment or realm for which the request is made.
     * @return The HTTP response status line indicating the result of the health check.
     * @throws CallbackException If an error occurs during the health check request.
     */
    public StatusLine get(String callbackUrl, String publisherId, String subscriberId, String environment) throws CallbackException {
        return doRequest(new HttpGet(callbackUrl), publisherId, subscriberId, environment);
    }

    private StatusLine doRequest(HttpRequestBase request, String publisherId, String subscriberId, String environment) throws CallbackException {
        if (environment == null || environment.isEmpty() || environment.equals(polarisConfig.getEnvironment())){
            environment = DEFAULT_REALM;
        }

        request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + oAuth2TokenCache.getToken(environment));
        request.setHeader("x-pubsub-publisher-id", publisherId);
        request.setHeader("x-pubsub-subscriber-id", subscriberId);

        try {
            try(var response = httpClient.execute(request)) {
                return response.getStatusLine();
            }
        } catch (RuntimeException | IOException e) {
            throw new CallbackException(String.format("Error %s at callback '%s'", e.getMessage(), request.getURI()), e);
        }
    }

    /**
     * Scheduled task to trigger the retrieval of OAuth2 access tokens every 4 hours.
     * This ensures that the tokens remain up-to-date and valid for health check requests.
     */
    @Scheduled(cron = "${polaris.oidc.cronTokenFetch}")
    protected void triggerTokenRetrieval() {
        oAuth2TokenCache.retrieveAllAccessTokens();
    }
}
