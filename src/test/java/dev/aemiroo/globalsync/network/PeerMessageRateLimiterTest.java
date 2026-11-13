package dev.aemiroo.globalsync.network;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PeerMessageRateLimiterTest {
    @Test
    void limitsMessageBurstPerPeer() {
        PeerMessageRateLimiter limiter = new PeerMessageRateLimiter(4, 1.0, 2.0, 10_000.0, 20_000.0);

        assertTrue(limiter.tryAcquire("peer-a", 100));
        assertTrue(limiter.tryAcquire("peer-a", 100));
        assertFalse(limiter.tryAcquire("peer-a", 100));
    }

    @Test
    void limitsPayloadBytesEvenWhenMessageCountIsAvailable() {
        PeerMessageRateLimiter limiter = new PeerMessageRateLimiter(4, 100.0, 100.0, 1_000.0, 1_500.0);

        assertTrue(limiter.tryAcquire("peer-a", 1_000));
        assertFalse(limiter.tryAcquire("peer-a", 600));
    }

    @Test
    void tracksPeersIndependentlyAndBoundsTrackedPeerState() {
        PeerMessageRateLimiter limiter = new PeerMessageRateLimiter(1, 1.0, 1.0, 1_000.0, 1_000.0);

        assertTrue(limiter.tryAcquire("peer-a", 100));
        assertFalse(limiter.tryAcquire("peer-b", 100));
        assertFalse(limiter.tryAcquire("peer-a", 100));
    }

    @Test
    void rejectsInvalidLimitsAndInputs() {
        assertThrows(IllegalArgumentException.class,
                () -> new PeerMessageRateLimiter(0, 1.0, 1.0, 1_000.0, 1_000.0));
        PeerMessageRateLimiter limiter = PeerMessageRateLimiter.defaults();
        assertFalse(limiter.tryAcquire("peer-a", 0));
        assertFalse(limiter.tryAcquire(" ", 100));
    }
}
