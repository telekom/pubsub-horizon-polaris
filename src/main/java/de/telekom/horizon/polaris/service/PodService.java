// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.service;

import de.telekom.eni.pandora.horizon.model.meta.CircuitBreakerMessage;
import de.telekom.horizon.polaris.cache.PolarisPodCache;
import de.telekom.horizon.polaris.config.PolarisConfig;
import de.telekom.horizon.polaris.exception.CouldNotDetermineWorkingSetException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Service for reading information about all Polaris pods.
 */
@Service
@Slf4j
public class PodService {

    private final PolarisPodCache polarisPodCache;
    private final PolarisConfig polarisConfig;

    public PodService(PolarisPodCache polarisPodCache, PolarisConfig polarisConfig) {
        this.polarisPodCache = polarisPodCache;
        this.polarisConfig = polarisConfig;
    }

    /**
     * Gets all pods from the {@link PolarisPodCache}.
     *
     * @return A list of all pods.
     * @throws CouldNotDetermineWorkingSetException If the {@link PolarisPodCache} is empty.
     */
    public List<String> getAllPods() throws CouldNotDetermineWorkingSetException {
        var allPods = polarisPodCache.getAllPods().stream().sorted().toList();

        if (allPods.isEmpty()) {
            throw new CouldNotDetermineWorkingSetException("{@link PolarisPodCache is empty. Could not determine working set");
        }

        return allPods;
    }

    /**
     * Checks if the callback URL should be handled by the current pod.
     *
     * @param callbackUrl The callback URL.
     * @return True if the callback URL should be handled by the current pod, false otherwise.
     * @throws CouldNotDetermineWorkingSetException If there is an issue determining the working set.
     */
    public boolean shouldCallbackUrlBeHandledByThisPod(String callbackUrl) throws CouldNotDetermineWorkingSetException {
        var allPods = getAllPods();
        var podCount = allPods.size();
        var ownIndex = allPods.indexOf(polarisConfig.getPodName());

        if (ownIndex == -1) {
            var message = String.format("Own Pod (%s) is not part of Polaris Pod Cache (%s). Could not determine working set...", polarisConfig.getPodName(), allPods);
            throw new CouldNotDetermineWorkingSetException(message);
        }

        // Creates the hash value of the callbackUrl (=> number) & compares it to our pod Index
        // if the hash equals to our index, we are working on it,
        // else, some other pod will work on it.
        // => As long as our pod has the same index, it will work on the same subscriptions with the that callbackUrl
        var callbackHashRest = Math.abs(callbackUrl.hashCode() % podCount);
        boolean isCallbackHashOurs = callbackHashRest == ownIndex;
        if(!isCallbackHashOurs) {
            log.debug("CallbackUrl gets processed by {} ({})", allPods.get(callbackHashRest), callbackHashRest);
            return false;
        }

        return true;
    }

    /**
     * Checks if a pod is present in the {@link PolarisPodCache}.
     *
     * @param pod The pod name.
     * @return True if the pod is present, false otherwise.
     * @throws CouldNotDetermineWorkingSetException If there is an issue determining the working set.
     */
    public boolean isPodInPodCache(String pod) throws CouldNotDetermineWorkingSetException {
        var allPods = getAllPods();
        return allPods.contains(pod);
    }

    /**
     * Checks if the {@link CircuitBreakerMessage} should be handled by the current pod.
     *
     * @param circuitBreakerMessage The {@link CircuitBreakerMessage}.
     * @return True if the {@link CircuitBreakerMessage} should be handled by the current pod, false otherwise.
     * @throws CouldNotDetermineWorkingSetException If there is an issue determining the working set.
     */
    public boolean shouldCircuitBreakerMessageBeHandledByThisPod(CircuitBreakerMessage circuitBreakerMessage) throws CouldNotDetermineWorkingSetException {
        String assignedPod = circuitBreakerMessage.getAssignedPodId();
        // what if no assigned pod ?

        boolean isUnassigned = StringUtils.isEmpty(assignedPod);
        boolean isOurCallbackHash = shouldCallbackUrlBeHandledByThisPod(circuitBreakerMessage.getCallbackUrl());
        boolean isAssignedToUs = polarisConfig.getPodName().equals(assignedPod);
        boolean isAssignedPodMissing = !isPodInPodCache(assignedPod);

        log.debug("circuitBreakerMessage: {}", circuitBreakerMessage);
        log.debug("isUnassigned: {}", isUnassigned);
        log.debug("isOurCallbackHash: {}", isOurCallbackHash);
        log.debug("isAssignedToUs: {}", isAssignedToUs);
        log.debug("isAssignedPodMissing: {}", isAssignedPodMissing);

        if(isOurCallbackHash) {
            return isUnassigned || isAssignedToUs || isAssignedPodMissing;
        }  else {
            return isAssignedToUs;
        }
    }
}
