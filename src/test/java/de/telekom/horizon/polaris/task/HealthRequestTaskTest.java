// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.task;


import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.horizon.polaris.cache.HealthCheckCache;
import de.telekom.horizon.polaris.component.HealthCheckRestClient;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CallbackException;
import de.telekom.horizon.polaris.service.CircuitBreakerCacheService;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.StatusLine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Spy;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.stream.Stream;

import static de.telekom.horizon.polaris.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Slf4j
class HealthRequestTaskTest {

    @Mock
    ThreadPoolService threadPoolService;
    @Spy
    HealthCheckCache healthCheckCache;

    @Mock
    CircuitBreakerCacheService circuitBreakerCache;

    @Mock
    HealthCheckRestClient restClient;
    @Mock
    PolarisConfig polarisConfig;

    @Mock
    StatusLine fakeStatusLine;

    HealthRequestTask healthRequestTask;

    @BeforeEach
    void prepare() throws CallbackException {
        when(fakeStatusLine.getStatusCode()).thenReturn(200);
        when(fakeStatusLine.getReasonPhrase()).thenReturn("Ok");

        when(restClient.get(anyString(), anyString(), anyString(), anyString())).thenReturn(fakeStatusLine);
        when(restClient.head(anyString(), anyString(), anyString(), anyString())).thenReturn(fakeStatusLine);

        when(polarisConfig.getSuccessfulStatusCodes()).thenReturn(List.of(200));

        var fakeCircuitBreakerMessages = MockGenerator.createFakeCircuitBreakerMessages(10, true, false);

        when(circuitBreakerCache.getCircuitBreakerMessage(anyString()))
                .then( iOM -> fakeCircuitBreakerMessages.stream().filter(e -> e.getSubscriptionId().equals(iOM.getArgument(0))).findFirst());

        when(threadPoolService.getHealthCheckCache()).thenReturn(healthCheckCache);
        when(threadPoolService.getCircuitBreakerCacheService()).thenReturn(circuitBreakerCache);
        when(threadPoolService.getPolarisConfig()).thenReturn(polarisConfig);
        when(threadPoolService.getRestClient()).thenReturn(restClient);

        for(var fakeCircuitBreakerMessage : fakeCircuitBreakerMessages) {
            healthCheckCache.add(fakeCircuitBreakerMessage.getCallbackUrl(), HttpMethod.HEAD, fakeCircuitBreakerMessage.getSubscriptionId());
            healthCheckCache.add(fakeCircuitBreakerMessage.getCallbackUrl(), HttpMethod.GET, fakeCircuitBreakerMessage.getSubscriptionId());
        }

    }

    private static Stream<HttpMethod> httpMethodsInclude() {
        return Stream.of(HttpMethod.HEAD, HttpMethod.GET);
    }

    private static Stream<HttpMethod> httpMethodsExclude() {
        return Stream.of(HttpMethod.PUT, HttpMethod.PATCH, HttpMethod.POST, HttpMethod.DELETE, HttpMethod.OPTIONS, HttpMethod.TRACE);
    }



    @ParameterizedTest()
    @MethodSource("httpMethodsExclude")
    @DisplayName("should throw IllegalArgumentException on unsupported HTTPMethod")
    void shouldThrowIllegalArgumentExceptionOnUnsupportedHTTPMethod(HttpMethod method)  {

        healthRequestTask = new HealthRequestTask(CALLBACK_URL, PUBLISHER_ID, SUBSCRIBER_ID, ENV, method, threadPoolService);

        assertThrows(IllegalArgumentException.class, () -> healthRequestTask.executeHealthCheckRequest( CALLBACK_URL, PUBLISHER_ID, SUBSCRIBER_ID, ENV, method ));
    }

    @ParameterizedTest()
    @MethodSource("httpMethodsInclude")
    @DisplayName("should NOT throw IllegalArgumentException on unsupported HTTPMethod")
    void shouldNotThrowIllegalArgumentExceptionOnUnsupportedHTTPMethod(HttpMethod method) {
        healthRequestTask = new HealthRequestTask(CALLBACK_URL, PUBLISHER_ID, SUBSCRIBER_ID, ENV, method, threadPoolService);

        assertDoesNotThrow(() -> healthRequestTask.executeHealthCheckRequest( CALLBACK_URL, PUBLISHER_ID, SUBSCRIBER_ID, ENV, method ));
    }

    @ParameterizedTest()
    @MethodSource("httpMethodsInclude")
    @DisplayName("should update HealthCheckData in Caches")
    void shouldUpdateHealthCheckDataInCaches(HttpMethod method)  {
        healthRequestTask = new HealthRequestTask(CALLBACK_URL, PUBLISHER_ID, SUBSCRIBER_ID, ENV, method, threadPoolService);

        healthRequestTask.call();

        // Update healthCache once (for this callbackUrl and HttpMethod)
        verify(healthCheckCache, times(1)).update(anyString(), any(HttpMethod.class), eq(200), eq("Ok"));

        // Update all circuit breakers with this callbackUrl and HttpMethod
        verify(circuitBreakerCache, times(10)).updateCircuitBreakerMessage(isA(CircuitBreakerMessage.class));
    }

    @ParameterizedTest()
    @MethodSource("httpMethodsInclude")
    @DisplayName("should call correct restClient method")
    void shouldCallCorrectRestClientMethod(HttpMethod method) throws CallbackException {
        healthRequestTask = new HealthRequestTask(CALLBACK_URL, PUBLISHER_ID, SUBSCRIBER_ID, ENV, method, threadPoolService);

        healthRequestTask.call();

        if(HttpMethod.GET.equals(method)) {
            verify(restClient, times(1)).get(eq(CALLBACK_URL), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV));
        } else { // HEAD
            verify(restClient, times(1)).head(eq(CALLBACK_URL), eq(PUBLISHER_ID), eq(SUBSCRIBER_ID), eq(ENV));
        }
    }

    @ParameterizedTest()
    @MethodSource("httpMethodsInclude")
    @DisplayName("should start HandleSuccessfulHealthRequestTask on successful response")
    void shouldStartHandleSuccessfulHealthRequestTaskOnSuccessfulResponse(HttpMethod method) {
        healthRequestTask = new HealthRequestTask(CALLBACK_URL, PUBLISHER_ID, SUBSCRIBER_ID, ENV, method, threadPoolService);

        Boolean wasSuccessful = healthRequestTask.call();
        assertTrue(wasSuccessful);
    }

    @ParameterizedTest()
    @MethodSource("httpMethodsInclude")
    @DisplayName("should start a new HealthRequestTask on unsuccessful response")
    void shouldStartANewHealthRequestTaskOnUnsuccessfulResponse(HttpMethod method) {
        healthRequestTask = new HealthRequestTask(CALLBACK_URL, PUBLISHER_ID, SUBSCRIBER_ID, ENV, method, threadPoolService);

        when(fakeStatusLine.getStatusCode()).thenReturn(503);
        when(fakeStatusLine.getReasonPhrase()).thenReturn("Service Unavailable");

        Boolean wasSuccessful = healthRequestTask.call();
        assertFalse(wasSuccessful);
    }


    @ParameterizedTest
    @CsvSource( value = {
            "0, 0",
            "1, 2",
            "2, 4",
            "3, 8",
            "4, 16",
            "5, 32",
            "6, 60",
            "7, 60",
            "8, 60",
            "9, 60",
            "10, 60",
    })
    @DisplayName("should calculate correct cooldown")
    void shouldSetIsThreadOpenToFalseOnThreadCancellation(int republishCount, int expectedCoodlownMins) {
        var cooldownDuration = HealthRequestTask.calculateCooldown(republishCount);

        assertFalse(cooldownDuration.isNegative());
        assertEquals(expectedCoodlownMins, cooldownDuration.toMinutes());
    }



}
