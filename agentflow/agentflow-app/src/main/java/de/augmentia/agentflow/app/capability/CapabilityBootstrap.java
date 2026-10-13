package de.augmentia.agentflow.app.capability;

import de.augmentia.quad.core.capability.Capability;
import de.augmentia.quad.core.capability.CapabilityIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** Indexes the {@link CapabilityCatalog} at startup so retrieval has data to work with. */
@Component
public class CapabilityBootstrap implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CapabilityBootstrap.class);

    private final CapabilityIndex index;
    private final boolean enabled;

    public CapabilityBootstrap(CapabilityIndex index,
                               @Value("${agentflow.capability.autoload:true}") boolean enabled) {
        this.index = index;
        this.enabled = enabled;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!enabled) {
            log.info("Capability autoload disabled; index left empty");
            return;
        }
        for (Capability capability : CapabilityCatalog.all()) {
            index.index(capability);
        }
        log.info("Indexed {} capabilities", CapabilityCatalog.all().size());
    }
}
