package dev.aemiroo.globalsync.network;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable allowlist binding a claimed server ID to one certificate fingerprint. */
public final class PeerAuthorizationRegistry {
    private final Map<String, PeerEndpoint> peers;

    public PeerAuthorizationRegistry(Collection<PeerEndpoint> endpoints) {
        Objects.requireNonNull(endpoints, "endpoints");
        Map<String, PeerEndpoint> copy = new LinkedHashMap<>();
        java.util.Set<String> fingerprints = new java.util.HashSet<>();
        for (PeerEndpoint endpoint : endpoints) {
            Objects.requireNonNull(endpoint, "endpoint");
            if (copy.putIfAbsent(endpoint.serverId(), endpoint) != null) {
                throw new IllegalArgumentException("Duplicate peer server ID: " + endpoint.serverId());
            }
            if (!fingerprints.add(endpoint.sha256Fingerprint())) {
                throw new IllegalArgumentException("A certificate fingerprint cannot authorize multiple peer IDs");
            }
        }
        this.peers = Map.copyOf(copy);
    }

    public PeerEndpoint requireAuthorized(SSLSession session, String claimedServerId)
            throws SSLPeerUnverifiedException {
        if (claimedServerId == null || claimedServerId.isBlank() || claimedServerId.length() > 64) {
            throw new SSLPeerUnverifiedException("Peer claimed an invalid server ID");
        }
        PeerEndpoint endpoint = peers.get(claimedServerId);
        if (endpoint == null) {
            throw new SSLPeerUnverifiedException("Peer server ID is not authorized");
        }
        PeerCertificateVerifier.verifySha256Fingerprint(session, endpoint.sha256Fingerprint());
        return endpoint;
    }

    public Collection<PeerEndpoint> peers() {
        return peers.values();
    }

    public PeerEndpoint find(String serverId) {
        return peers.get(serverId);
    }

    public int size() {
        return peers.size();
    }
}
