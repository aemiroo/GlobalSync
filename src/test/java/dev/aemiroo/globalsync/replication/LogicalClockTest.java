package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LogicalClockTest {
    @Test
    void localTicksAndRemoteObservationsAdvancePastKnownEvents() {
        LogicalClock clock = new LogicalClock(4);
        assertEquals(5, clock.tick());
        assertEquals(10, clock.observe(9));
        assertEquals(11, clock.tick());
        assertEquals(11, clock.current());
    }

    @Test
    void rejectsInvalidAndExhaustedClockValues() {
        assertThrows(IllegalArgumentException.class, () -> new LogicalClock(-1));
        assertThrows(IllegalArgumentException.class, () -> new LogicalClock().observe(0));
        LogicalClock exhausted = new LogicalClock(Long.MAX_VALUE);
        assertThrows(IllegalStateException.class, exhausted::tick);
        assertThrows(IllegalStateException.class, () -> exhausted.observe(1));
    }
}
