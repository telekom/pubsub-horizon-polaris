package de.telekom.horizon.polaris.service;

import de.telekom.eni.pandora.horizon.kubernetes.PodResourceListener;
import de.telekom.eni.pandora.horizon.kubernetes.SubscriptionResourceListener;
import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.cache.PolarisPodCache;
import de.telekom.horizon.polaris.component.CircuitBreakerManager;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CouldNotDetermineWorkingSetException;
import de.telekom.horizon.polaris.kubernetes.PodResourceEventHandler;
import de.telekom.horizon.polaris.util.MockGenerator;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.env.Environment;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static de.telekom.horizon.polaris.TestConstants.POD_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Slf4j
class PolarisServiceTest {

    @SpyBean
    PolarisPodCache polarisPodCache;

    PodResourceEventHandler podResourceEventHandler;

    @Mock
    SubscriptionResourceListener subscriptionResourceListener;
    @Mock
    PodResourceListener podResourceListener;
    @Mock
    CircuitBreakerManager circuitBreakerManager;

    @SpyBean
    Environment environment;
    @Mock
    PolarisConfig polarisConfig;
    @Mock
    CircuitBreakerCacheService circuitBreakerCacheService;

    PolarisService polarisService;
    PodService podService;
    Pod fakePod;

    PolarisServiceTest() {
    }

    @BeforeEach
    public void prepare() {
        var threadPoolService = mock(ThreadPoolService.class);
        var partialSubscriptionCache = mock(PartialSubscriptionCache.class);

        polarisService = spy(new PolarisService(subscriptionResourceListener, podResourceListener, polarisPodCache, partialSubscriptionCache));
        podService = spy(new PodService(polarisPodCache, polarisConfig));

//        when(threadPoolService.getPolarisService()).thenReturn(polarisService);
        when(threadPoolService.getPolarisConfig()).thenReturn(polarisConfig);
        when(threadPoolService.getPartialSubscriptionCache()).thenReturn(partialSubscriptionCache);
        when(threadPoolService.getCircuitBreakerCacheService()).thenReturn(circuitBreakerCacheService);

        circuitBreakerManager = spy(new CircuitBreakerManager(threadPoolService));
        podResourceEventHandler = new PodResourceEventHandler(polarisPodCache, circuitBreakerManager);

        polarisPodCache.getAllPods().forEach(e -> polarisPodCache.remove(e));
        fakePod = MockGenerator.createFakePod(POD_NAME);
    }

    @Test
    @DisplayName("should throw CouldNotDetermineWorkingSet")
    void shouldThrowCouldNotDetermineWorkingSet() {
//        podResourceEventHandler.onAdd(fakePod);
        assertThrowsExactly(CouldNotDetermineWorkingSetException.class, () -> podService.getAllPods());
    }

    @Test
    @DisplayName("should get all pods")
    void shouldGetAllPods() throws CouldNotDetermineWorkingSetException {
        podResourceEventHandler.onAdd(fakePod);
        List<String> pods = podService.getAllPods();
        log.info("pods: {}", pods);
        assertEquals(1, pods.size());
        assertEquals( fakePod.getMetadata().getName(), pods.get(0));
    }

    @RepeatedTest(3)
    @DisplayName("should balance cbMessages evenly on polaris pods")
    void shouldBalanceEvenlyOnPolarisPods() throws CouldNotDetermineWorkingSetException {
        final var POD_COUNT = 10;
        final var MESSAGE_COUNT = 10000;
        final double DEVIATION = 0.1; // 10%
        final var optimalDistribution = MESSAGE_COUNT / POD_COUNT;
        final int allowedDeviation = (int) Math.round(optimalDistribution * DEVIATION);
        final int atLeast = optimalDistribution - allowedDeviation;
        final int atMost = optimalDistribution + allowedDeviation;

        var fakePods = MockGenerator.createFakePods(POD_COUNT);
        var fakePodNamesSet = fakePods.stream().map(pod -> pod.getMetadata().getName()).collect(Collectors.toSet());
        when(polarisPodCache.getAllPods()).thenReturn(fakePodNamesSet);

        List<CircuitBreakerMessage> fakeCbMessages = MockGenerator.createFakeCircuitBreakerMessages(MESSAGE_COUNT, true, true);
        String testPodName = fakePods.get(0).getMetadata().getName();

        when(polarisConfig.getPodName()).thenReturn(testPodName);

        List<Boolean> responses = new ArrayList<>();

        for(var fakeCbMessage: fakeCbMessages) {
            responses.add(podService.shouldCallbackUrlBeHandledByThisPod(fakeCbMessage.getCallbackUrl()));
        }
        var trueCount = responses.stream().filter(b -> b).count();
        var falseCount = responses.stream().filter(b -> !b).count();

        log.info("trueCount: {}, falseCount: {}, sum: {}", trueCount, falseCount, responses.size());
        log.info("atLeast: {}", atLeast);
        log.info("atMost: {}", atMost);
        assertTrue(trueCount >= atLeast);
        assertTrue(trueCount <= atMost);

    }
}