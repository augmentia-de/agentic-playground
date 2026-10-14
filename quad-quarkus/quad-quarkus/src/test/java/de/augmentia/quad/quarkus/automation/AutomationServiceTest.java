package de.augmentia.quad.quarkus.automation;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class AutomationServiceTest {

    @Test
    void resultText_isRealOrEmpty() throws Exception {
        AutomationService svc = new AutomationService();
        Method m = AutomationService.class.getDeclaredMethod("lastAssistantText", String.class);
        m.setAccessible(true);
        Object result = m.invoke(svc, "sess-1");
        assertFalse(String.valueOf(result).contains("[session-"));
        assertFalse(String.valueOf(result).contains("transcript]"));
    }
}
