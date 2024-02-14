package de.telekom.horizon.polaris.component;

import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerHealthCheck;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerStatus;
import de.telekom.horizon.polaris.service.ThreadPoolService;
import de.telekom.horizon.polaris.util.MockGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static de.telekom.horizon.polaris.TestConstants.SUBSCRIPTION_ID;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Slf4j
class HealthCacheCleanerTest {

    @Mock
    ThreadPoolService threadPoolService;
    HealthCacheCleaner healthCacheCleaner;

    @BeforeEach
    void prepare() {
        threadPoolService = MockGenerator.mockThreadPoolService();

        // Delete entries older 90 mins
        when(MockGenerator.polarisConfig.getRequestCooldownResetMins()).thenReturn(90);

        healthCacheCleaner = new HealthCacheCleaner(threadPoolService);
    }

    @ParameterizedTest
    @CsvSource({
            "1, true, false",
            "2, false, false",
            "4, true, false",
            "8, false, false",
            "16, true, false",
            "32, false, false",
            "64, true, false",
            "128, false, true",
            "256, true, true",
            "512, false, true",
            "1028, true, true"
    })
    @DisplayName("Should delete old entries")
    void shouldDeleteOldEntries(int minutesAgo, boolean isThreadOpen, boolean shouldRemove) {
        HttpMethod httpMethod = HttpMethod.HEAD;

        String callbackUrl = RandomStringUtils.random(minutesAgo, true, false);
        Date dateXAgo = Date.from(Instant.now().minus( minutesAgo, ChronoUnit.MINUTES));
        addToHealthCache(callbackUrl, httpMethod, dateXAgo, isThreadOpen);

        healthCacheCleaner.cleanHealthCache();

        Assertions.assertEquals(shouldRemove, MockGenerator.healthCheckCache.get(callbackUrl, httpMethod).isEmpty());
        if (shouldRemove && isThreadOpen) {
            verify(MockGenerator.circuitBreakerCache, times(1)).updateCircuitBreakerStatus(eq(SUBSCRIPTION_ID), eq(CircuitBreakerStatus.OPEN));
        }
    }

    private void addToHealthCache(String callbackUrl, HttpMethod httpMethod, Date date, boolean isThreadOpen) {
        CircuitBreakerHealthCheck lastHealthCheckWithOldTimestamp = new CircuitBreakerHealthCheck(date, date, 503, "REASON");

        MockGenerator.healthCheckCache.add(callbackUrl, httpMethod, SUBSCRIPTION_ID);
        MockGenerator.healthCheckCache.update(callbackUrl, httpMethod, isThreadOpen);
        MockGenerator.healthCheckCache.update(callbackUrl, httpMethod, lastHealthCheckWithOldTimestamp);
    }
}
