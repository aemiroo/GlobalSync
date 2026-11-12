package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SequenceClockTest {
    @Test
    void generatesIncreasingSequences() {
        SequenceClock clock = new SequenceClock();
        assertEquals(1, clock.next());
        assertEquals(2, clock.next());
        assertEquals(2, clock.current());
    }

    @Test
    void resumesAfterTheLastPersistedSequence() {
        SequenceClock clock = new SequenceClock(41);
        assertEquals(42, clock.next());
        assertEquals(42, clock.current());
    }

    @Test
    void rejectsANegativeInitialSequence() {
        assertThrows(IllegalArgumentException.class, () -> new SequenceClock(-1));
    }
}
