package dev.aemiroo.globalsync.replication;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lamport logical clock for ordering events across shared-world writers.
 *
 * <p>Local events call {@link #tick()}. A node calls {@link #observe(long)}
 * when it accepts a remote event, ensuring subsequent local events sort after
 * the event it observed. Persist the value alongside every accepted event and
 * restore the maximum persisted value on startup.</p>
 */
public final class LogicalClock {
    private final AtomicLong value;

    public LogicalClock() {
        this(0);
    }

    public LogicalClock(long initialValue) {
        if (initialValue < 0) {
            throw new IllegalArgumentException("initialValue must not be negative");
        }
        value = new AtomicLong(initialValue);
    }

    public long tick() {
        return value.updateAndGet(current -> {
            if (current == Long.MAX_VALUE) {
                throw new IllegalStateException("Logical clock exhausted");
            }
            return current + 1;
        });
    }

    /**
     * Records receipt of a remote event and advances beyond both known clocks.
     */
    public long observe(long remoteValue) {
        if (remoteValue < 1) {
            throw new IllegalArgumentException("remoteValue must be positive");
        }
        return value.updateAndGet(current -> {
            long maximum = Math.max(current, remoteValue);
            if (maximum == Long.MAX_VALUE) {
                throw new IllegalStateException("Logical clock exhausted");
            }
            return maximum + 1;
        });
    }

    public long current() {
        return value.get();
    }
}
