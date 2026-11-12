package dev.aemiroo.globalsync.replication;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates monotonically increasing sequence numbers for events emitted by this node.
 * This is a local ordering primitive, not a global consensus algorithm.
 */
public final class SequenceClock {
    private final AtomicLong sequence = new AtomicLong();

    public long next() {
        return sequence.incrementAndGet();
    }

    public long current() {
        return sequence.get();
    }
}
