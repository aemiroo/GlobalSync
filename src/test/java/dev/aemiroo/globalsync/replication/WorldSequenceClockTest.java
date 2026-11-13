package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WorldSequenceClockTest {
    @Test
    void advancesWorldsIndependently() {
        WorldSequenceClock clock = new WorldSequenceClock();
        assertEquals(1, clock.next("overworld"));
        assertEquals(1, clock.next("nether"));
        assertEquals(2, clock.next("overworld"));
        assertEquals(1, clock.current("nether"));
    }

    @Test
    void restoresEachWorldFromItsOwnPersistedValue() {
        WorldSequenceClock clock = new WorldSequenceClock(Map.of("overworld", 9L, "nether", 3L));
        assertEquals(10, clock.next("overworld"));
        assertEquals(4, clock.next("nether"));
    }

    @Test
    void rejectsBlankWorldIdsAndNegativeRestoredValues() {
        WorldSequenceClock clock = new WorldSequenceClock();
        assertThrows(IllegalArgumentException.class, () -> clock.next(" "));
        assertThrows(IllegalArgumentException.class,
                () -> new WorldSequenceClock(Map.of("overworld", -1L)));
    }
}
