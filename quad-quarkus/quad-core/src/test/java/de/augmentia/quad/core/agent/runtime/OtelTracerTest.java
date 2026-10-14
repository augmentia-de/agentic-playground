package de.augmentia.quad.core.agent.runtime;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tool spans/events must carry the tool identity (it was silently dropped). */
class OtelTracerTest {

    @Test
    void addEvent_includesToolName() {
        OTelAgentTracer tracer = new OTelAgentTracer();
        Span span = mock(Span.class);

        tracer.addEvent(span, "tool.started", "readFile");

        ArgumentCaptor<Attributes> captor = ArgumentCaptor.forClass(Attributes.class);
        verify(span).addEvent(eq("tool.started"), captor.capture());
        assertThat(captor.getValue().get(AttributeKey.stringKey("tool.name"))).isEqualTo("readFile");
    }

    @Test
    void addEvent_withoutToolName_hasNoAttributes() {
        OTelAgentTracer tracer = new OTelAgentTracer();
        Span span = mock(Span.class);

        tracer.addEvent(span, "agent.started", null);

        verify(span).addEvent("agent.started");
    }
}
