package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SequenceClockTest {
    @Test
    void generatesIncreasingSequences() {
        SequenceClock clock = new SequenceClock();
        assertEquals(1, clock.next());
        assertEquals(2, clock.next());
        assertEquals(2, clock.current());
    }
}
