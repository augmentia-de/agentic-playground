package de.augmentia.quad.core.tool.sandbox;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A stderr/stdout flood must not grow the retained sandbox output without bound. */
class BoundedOutputTest {

    @Test
    void stderrFloodRemainsBounded() {
        BoundedOutput output = new BoundedOutput();
        for (int i = 0; i < 200_000; i++) {
            output.add("[stderr] line " + i);
        }

        assertThat(output.lineCount()).isLessThanOrEqualTo(BoundedOutput.MAX_LINES);
        assertThat(output.byteCount()).isLessThanOrEqualTo((long) BoundedOutput.MAX_BYTES * 2);
    }

    @Test
    void keepsTheMostRecentLines() {
        BoundedOutput output = new BoundedOutput();
        for (int i = 0; i < 1000; i++) {
            output.add("line-" + i);
        }

        assertThat(output.join()).contains("line-999");
        assertThat(output.lineCount()).isLessThanOrEqualTo(BoundedOutput.MAX_LINES);
    }
}
