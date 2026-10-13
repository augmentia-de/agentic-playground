package de.augmentia.agentflow.app.capability;

import de.augmentia.quad.core.capability.Capability;
import de.augmentia.quad.core.capability.CapabilitySearch;
import de.augmentia.quad.core.capability.HnswCapabilityIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CapabilitySearchTest {

    private HnswCapabilityIndex index;
    private CapabilitySearch search;

    @BeforeEach
    void setUp() {
        index = new HnswCapabilityIndex();
        CapabilityCatalog.all().forEach(index::index);
        search = new CapabilitySearch(index, 100, 10);
    }

    @Test
    void findsWebTools() {
        List<Capability> results = search.search("web", 5);
        assertThat(results).extracting(Capability::name).contains("webSearch", "webfetch");
    }

    @Test
    void findsFileTools() {
        assertThat(search.search("file", 5)).extracting(Capability::name)
                .contains("readFile", "writeFile");
    }

    @Test
    void tenantFilterHidesRestrictedCapabilities() {
        index.index(new Capability("secret", "internal secret tool", "x", "quad", "SECRET",
                Set.of("tenant-a"), 0.9));
        assertThat(search.search("secret", 5, "tenant-a")).extracting(Capability::name).contains("secret");
        assertThat(search.search("secret", 5, "tenant-b")).extracting(Capability::name).doesNotContain("secret");
    }

    @Test
    void catalogIsNotEmptyAndUnique() {
        assertThat(CapabilityCatalog.all()).isNotEmpty();
        assertThat(CapabilityCatalog.all()).extracting(Capability::name).doesNotHaveDuplicates();
    }
}
