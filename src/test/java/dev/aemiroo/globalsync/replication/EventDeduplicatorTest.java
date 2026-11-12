package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EventDeduplicatorTest {
    @Test
    void acceptsAnEventOnlyOnceWhileItRemainsInTheWindow() {
        EventDeduplicator deduplicator = new EventDeduplicator(2);
        UUID id = UUID.randomUUID();
        assertTrue(deduplicator.markIfNew(id));
        assertFalse(deduplicator.markIfNew(id));
        assertEquals(1, deduplicator.size());
    }

    @Test
    void evictsOldEntriesWhenCapacityIsReached() {
        EventDeduplicator deduplicator = new EventDeduplicator(1);
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        assertTrue(deduplicator.markIfNew(first));
        assertTrue(deduplicator.markIfNew(second));
        assertEquals(1, deduplicator.size());
    }

    @Test
    void rejectsInvalidCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new EventDeduplicator(0));
    }
}
