// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.kubernetes;

import de.telekom.horizon.polaris.cache.PolarisPodCache;
import de.telekom.horizon.polaris.component.CircuitBreakerManager;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PodResourceEventHandler implements ResourceEventHandler<Pod> {
    private static final String POD_PHASE_RUNNING = "Running";
    private final PolarisPodCache polarisPodCache;
    private final CircuitBreakerManager circuitBreakerManager;

    public PodResourceEventHandler(PolarisPodCache polarisPodCache, @Lazy() CircuitBreakerManager circuitBreakerManager) {
        this.polarisPodCache = polarisPodCache;
        this.circuitBreakerManager = circuitBreakerManager;
    }

    @Override
    public void onAdd(Pod pod) {
        var podIsRunning = podIsRunningAndReady(pod);
        log.info("Add pod: {} with running = {}", pod.getMetadata().getName(), podIsRunning);

        if (podIsRunning) {
            polarisPodCache.add(pod.getMetadata().getName());
        }
    }

    @Override
    public void onUpdate(Pod oldPod, Pod newPod) {
        var podIsRunning = podIsRunningAndReady(newPod);
        log.info("Update pod: {} with running = {}", newPod.getMetadata().getName(), podIsRunning);

        if (podIsRunning) {
            polarisPodCache.add(newPod.getMetadata().getName());
        } else {
            polarisPodCache.remove(newPod.getMetadata().getName());
        }
    }

    @Override
    public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
        log.info("Delete pod: {}", pod.getMetadata().getName());
        polarisPodCache.remove(pod.getMetadata().getName());
        circuitBreakerManager.rebalanceFromRemovedPod(pod.getMetadata().getName());
    }

    private boolean podIsRunningAndReady(Pod pod) {
        // No Status -> Pod is seen as not ready
        if (pod.getStatus() == null) {
            return false;
        }

        // Pod Phase is not equal to "Running" -> Pod is seen as not ready
        if (pod.getStatus().getPhase() == null || !pod.getStatus().getPhase().equalsIgnoreCase(POD_PHASE_RUNNING)) {
            return false;
        }

        var containerStatuses = pod.getStatus().getContainerStatuses();

        // No container statuses -> Pod is seen as not ready
        if (containerStatuses == null || containerStatuses.isEmpty()) {
            return false;
        }

        // Only if all containers in pod are started and ready the pod is seen as ready
        return containerStatuses.stream().allMatch(c -> c.getReady() && c.getStarted());
    }
}
