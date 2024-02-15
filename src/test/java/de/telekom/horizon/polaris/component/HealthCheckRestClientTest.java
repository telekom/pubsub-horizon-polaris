// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.auth.OAuth2TokenCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CallbackException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.http.HttpTimeoutException;

import static de.telekom.horizon.polaris.TestConstants.CALLBACK_URL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HealthCheckRestClientTest {

    @Mock
    OAuth2TokenCache tokenCache;

    @Mock
    CloseableHttpClient httpClient;

    @Mock
    PolarisConfig polarisConfig;

    @Mock
    CloseableHttpResponse closeableHttpResponse;

    @Mock
    StatusLine statusLine;

    HealthCheckRestClient restClient;

    @BeforeEach
    void prepare() {
        doNothing().when(tokenCache).retrieveAllAccessTokens();
        restClient = Mockito.spy(new HealthCheckRestClient(tokenCache, httpClient, polarisConfig));
        restClient.triggerTokenRetrieval();
    }

    @Test
    @DisplayName("head should throw CallbackException with NullPointerException for invalid uri")
    void headShouldThrowCallbackExceptionWithNullPointerExceptionForInvalidUri() {
        try {
            restClient.head("Invalid", "pantest-publisher", "pantest-subscriber", "default");
            fail();
        } catch (CallbackException e) {
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof NullPointerException);
        }
    }

    @Test
    @DisplayName("head should throw CallbackException for timeout")
    void headShouldThrowCallbackExceptionForTimeout() throws IOException {
        when(httpClient.execute(any(HttpHead.class))).thenThrow(new HttpTimeoutException("Timeout"));
        try {
            restClient.head(CALLBACK_URL, "pantest-publisher", "pantest-subscriber", "default");
            fail();
        } catch (CallbackException e) {
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof HttpTimeoutException);
        }
    }

    @Test
    @DisplayName("execute head request successful")
    void executeHeadRequestSuccessful() throws IOException {
        when(httpClient.execute(any(HttpHead.class))).thenReturn(closeableHttpResponse);
        when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);

        assertDoesNotThrow(() -> {
                restClient.head(CALLBACK_URL, "pantest-publisher", "pantest-subscriber", "default");
        });

        verify(httpClient, times(1)).execute(any(HttpHead.class));
        verify(closeableHttpResponse, times(1)).getStatusLine();
    }

    @Test
    @DisplayName("execute get request successful")
    void executeGetRequestSuccessful() throws IOException {
        when(httpClient.execute(any(HttpGet.class))).thenReturn(closeableHttpResponse);
        when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);

        assertDoesNotThrow(() -> {
            restClient.get(CALLBACK_URL, "pantest-publisher", "pantest-subscriber", "default");
        });

        verify(httpClient, times(1)).execute(any(HttpGet.class));
        verify(closeableHttpResponse, times(1)).getStatusLine();
    }
}