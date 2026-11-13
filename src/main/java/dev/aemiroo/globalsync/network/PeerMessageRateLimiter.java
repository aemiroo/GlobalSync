package dev.aemiroo.globalsync.network;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded per-peer token buckets for inbound replication frames.
 *
 * Both message count and payload bytes are limited so a peer cannot exhaust
 * the inbox worker with many tiny frames or a small number of large frames.
 */
public final class PeerMessageRateLimiter {
    private final int maxTrackedPeers;
    private final double eventsPerSecond;
    private final double eventBurst;
    private final double bytesPerSecond;
    private final double byteBurst;
    private final Map<String, Bucket> buckets = new HashMap<>();

    public PeerMessageRateLimiter(int maxTrackedPeers, double eventsPerSecond,
            double eventBurst, double bytesPerSecond, double byteBurst) {
        if (maxTrackedPeers < 1 || maxTrackedPeers > 64)
            throw new IllegalArgumentException("maxTrackedPeers must be between 1 and 64");
        if (!positiveFinite(eventsPerSecond) || !positiveFinite(eventBurst)
                || !positiveFinite(bytesPerSecond) || !positiveFinite(byteBurst))
            throw new IllegalArgumentException("Rate and burst limits must be finite and positive");
        this.maxTrackedPeers = maxTrackedPeers;
        this.eventsPerSecond = eventsPerSecond;
        this.eventBurst = eventBurst;
        this.bytesPerSecond = bytesPerSecond;
        this.byteBurst = byteBurst;
    }

    public static PeerMessageRateLimiter defaults() {
        return new PeerMessageRateLimiter(64, 100.0, 200.0, 1_048_576.0, 2_097_152.0);
    }

    /**
     * Attempts to consume one message token and {@code payloadBytes} byte tokens.
     * Uses monotonic time and keeps at most one bucket per configured peer.
     */
    public synchronized boolean tryAcquire(String peerId, int payloadBytes) {
        Objects.requireNonNull(peerId, "peerId");
        if (peerId.isBlank() || payloadBytes < 1) return false;

        Bucket bucket = buckets.get(peerId);
        long now = System.nanoTime();
        if (bucket == null) {
            if (buckets.size() >= maxTrackedPeers) return false;
            bucket = new Bucket(eventBurst, byteBurst, now);
            buckets.put(peerId, bucket);
        } else {
            double elapsedSeconds = Math.max(0L, now - bucket.lastRefillNanos) / 1_000_000_000.0;
            bucket.eventTokens = Math.min(eventBurst,
                    bucket.eventTokens + elapsedSeconds * eventsPerSecond);
            bucket.byteTokens = Math.min(byteBurst,
                    bucket.byteTokens + elapsedSeconds * bytesPerSecond);
            bucket.lastRefillNanos = now;
        }

        if (bucket.eventTokens < 1.0 || bucket.byteTokens < payloadBytes) return false;
        bucket.eventTokens -= 1.0;
        bucket.byteTokens -= payloadBytes;
        return true;
    }

    private static boolean positiveFinite(double value) {
        return Double.isFinite(value) && value > 0.0;
    }

    private static final class Bucket {
        private double eventTokens;
        private double byteTokens;
        private long lastRefillNanos;

        private Bucket(double eventTokens, double byteTokens, long now) {
            this.eventTokens = eventTokens;
            this.byteTokens = byteTokens;
            this.lastRefillNanos = now;
        }
    }
}
