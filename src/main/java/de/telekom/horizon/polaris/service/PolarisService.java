package de.telekom.horizon.polaris.service;

import de.telekom.eni.pandora.horizon.kubernetes.PodResourceListener;
import de.telekom.eni.pandora.horizon.kubernetes.SubscriptionResourceListener;
import de.telekom.horizon.polaris.cache.PartialSubscriptionCache;
import de.telekom.horizon.polaris.cache.PolarisPodCache;
import de.telekom.horizon.polaris.model.PartialSubscription;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class PolarisService {
    private final SubscriptionResourceListener subscriptionResourceListener;
    private final PodResourceListener podResourceListener;
    private final PolarisPodCache polarisPodCache;
    private final PartialSubscriptionCache partialSubscriptionCache;
    private boolean arePodsLoaded;
    private boolean areSubscriptionsLoaded;
    @Setter
    private Runnable fullySyncedCallback;

    public PolarisService(SubscriptionResourceListener subscriptionResourceListener,
                          PodResourceListener podResourceListener,
                          PolarisPodCache polarisPodCache,
                          PartialSubscriptionCache partialSubscriptionCache) {
        this.subscriptionResourceListener = subscriptionResourceListener;
        this.podResourceListener = podResourceListener;
        this.partialSubscriptionCache = partialSubscriptionCache;
        this.polarisPodCache = polarisPodCache;

        arePodsLoaded = false;
        areSubscriptionsLoaded = false;
    }

    @PostConstruct
    public void initListeners() {
        if (subscriptionResourceListener != null) {
            log.info("Start subscriptionResourceListener");
            var subscriptions = subscriptionResourceListener.getAllSubscriptions();
            log.info("Loaded {} subscription resources from start...", subscriptions.size());
            for(var subscription : subscriptions) {
                partialSubscriptionCache.add(PartialSubscription.fromSubscriptionResource(subscription));
            }
            areSubscriptionsLoaded = true;

            subscriptionResourceListener.start();
        }

        if (podResourceListener != null) {
            log.info("Start podResourceListener");
            var pods = podResourceListener.getAllPods();
            log.info("Loaded {} pod resources from start...", pods.size());
            for(var pod : pods) {
                polarisPodCache.add(pod.getMetadata().getName());
            }
            arePodsLoaded = true;

            podResourceListener.start();
        }


        if(areResourcesFullySynced() && fullySyncedCallback != null) {
            Executors.newSingleThreadExecutor().execute(fullySyncedCallback);
        }
    }

    public boolean areResourcesFullySynced() {
        return arePodsLoaded && areSubscriptionsLoaded;
    }


}
