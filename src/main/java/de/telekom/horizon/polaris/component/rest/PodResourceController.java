package de.telekom.horizon.polaris.component.rest;


import de.telekom.horizon.polaris.cache.PolarisPodCache;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@AllArgsConstructor
@RestController()
@RequestMapping("pods")
public class PodResourceController {
    private final PolarisPodCache polarisPodCache;
    /**
     * Retrieves all Polaris pods.
     *
     * @return Collection of Polaris pod names.
     */
    @GetMapping()
    public Collection<String> getAll() {
        return polarisPodCache.getAllPods();
    }
}
