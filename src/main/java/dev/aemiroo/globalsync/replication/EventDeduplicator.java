package dev.aemiroo.globalsync.replication;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Bounded in-memory duplicate filter. A durable replication log must still enforce
 * idempotency across restarts; this cache only protects the current process.
 */
public final class EventDeduplicator {
    private final int capacity;
    private final Map<UUID, Boolean> seen;

    public EventDeduplicator(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.seen = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<UUID, Boolean> eldest) {
                return size() > EventDeduplicator.this.capacity;
            }
        };
    }

    public synchronized boolean markIfNew(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId");
        if (seen.containsKey(eventId)) {
            return false;
        }
        seen.put(eventId, Boolean.TRUE);
        return true;
    }

    public synchronized int size() {
        return seen.size();
    }
}
