package dev.aemiroo.globalsync.replication;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates monotonically increasing sequence numbers for events emitted by this node.
 * The initial value should be restored from the durable journal after a restart.
 * This is a per-node ordering primitive, not a global consensus algorithm.
 */
public final class SequenceClock {
    private final AtomicLong sequence;

    public SequenceClock() {
        this(0);
    }

    public SequenceClock(long initialSequence) {
        if (initialSequence < 0) {
            throw new IllegalArgumentException("initialSequence must not be negative");
        }
        this.sequence = new AtomicLong(initialSequence);
    }

    public long next() {
        return sequence.incrementAndGet();
    }

    public long current() {
        return sequence.get();
    }
}
