package de.augmentia.quad.quarkus.ui;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SystemControllerTest {

    @Test
    void extract_nestedJson() {
        String payload = "{\"outer\":{\"inner\":\"value\",\"durationMs\":42}}";
        assertEquals("value", SystemController.extract(payload, "outer.inner"));
        assertEquals("42", SystemController.extract(payload, "outer.durationMs"));
    }

    @Test
    void metrics_markedStub() {
        // compile check only; runtime needs CDI
        
    }
}
