# Security policy

## Scope and security claims

GlobalSync is experimental software. Peer networking is disabled by default. When explicitly enabled, this version can establish authenticated, idle TLS peer channels only; it does not exchange replication events or apply remote world state.

No plugin can guarantee that it is impossible to backdoor. GlobalSync aims to make unauthorized behavior difficult to introduce, constrain the impact of a compromised node, and make releases auditable. A clean build or a passing test suite is not proof that source code is benign.

## Current trust boundaries

- The Minecraft server process and its administrators are trusted. A server operator, host administrator, JVM agent, or another plugin with equivalent process access can alter GlobalSync behavior or read its secrets.
- The local journal is trusted only to the extent that the server account and filesystem are protected. SQLite durability is not tamper-proof storage.
- Peer certificates authenticate possession of a private key. The configured trust store and per-peer SHA-256 fingerprint allowlist must both authorize a peer.
- A valid peer is not globally authoritative. Future protocol handlers must validate message type, schema, world allowlist, origin identity, sequence, size, and authorization independently.
- A compromised regional node must not be allowed to execute commands, upload code, select arbitrary file paths, or change another node's configuration.

## Current safeguards

- Peer networking is disabled by default.
- TLS 1.3 only, mutual certificate authentication, certificate-chain validation, explicit leaf-certificate SHA-256 pinning, and client hostname verification are required for peer handshakes.
- The HELLO/READY handshake has a strict field allowlist, protocol-version check, and fresh nonce confirmation.
- Inbound/outbound connection workers are bounded; handshake reads have timeouts; pending and active sockets are closed during shutdown.
- Length-prefixed frames are bounded before payload allocation.
- Post-handshake application data is rejected. No replication message types or remote operations are implemented.
- TLS private keys and store passwords must remain outside source control. Supply passwords through the configured environment variables.
- The plugin has no supported remote command or arbitrary code execution feature.

## Required controls before replication messages are enabled

Do not implement or enable application-message exchange until all of these exist and are reviewed:

1. A strict message allowlist and schema validation. Reject unknown message types, unsupported protocol versions, oversized fields, invalid IDs, invalid world names, and unauthorized origins.
2. Per-peer rate limits, connection limits, read/write timeouts, bounded queues, and backpressure.
3. Durable receiver-side deduplication and acknowledgements only after commit.
4. A safe failure mode: malformed or unauthorized messages are rejected; they never trigger world changes.
5. Integration tests for invalid certificates, untrusted peers, wrong fingerprints, replay, malformed frames, oversized messages, and disconnects.
6. A documented certificate rotation and revocation procedure.
7. Explicit authorization for each shared world and each event type; a valid TLS peer must not gain unrestricted world-write authority.

Never add remote console, command dispatch, class loading, scripting, plugin installation, arbitrary file access, or configuration mutation to the replication protocol.

## Release and repository hygiene

- Review every source and dependency change before merging.
- Protect the default branch with required review and CI checks. Do not allow unreviewed direct pushes.
- Pin GitHub Actions to immutable commit SHAs in release workflows and grant workflows only the permissions they need.
- Generate an SBOM and scan dependencies before publishing a release. Review transitive dependencies as well as direct dependencies.
- Publish release artifacts from tagged commits and retain build provenance/attestations where available.
- Verify release checksums and provenance before installing a jar. Never install a jar merely because it has the expected filename.
- Build from a clean checkout with the documented Java and Maven versions. Compare the reviewed source commit with the release tag.
- Keep private keys, trust-store passwords, tokens, and production configuration out of Git. Rotate any secret that was committed or exposed.

## Reporting a vulnerability

Do not publish exploit details or secrets in a public issue. Use GitHub's private vulnerability reporting for this repository if enabled, or contact the repository maintainer privately with the affected version, impact, and a reproducible description.

## Operator checklist

- Run GlobalSync under a dedicated, unprivileged server account.
- Restrict filesystem permissions on the plugin data directory and TLS stores.
- Restrict inbound and outbound firewall rules to known peer addresses.
- Keep the Paper server, Java runtime, operating system, and other plugins patched.
- Audit the full plugin set; another plugin running in the same JVM can potentially access GlobalSync data and memory.
- Back up the journal and TLS material securely, and test restoration.
- Leave peer networking disabled unless you are deliberately testing the authenticated connection layer with operator-provisioned certificates and firewall restrictions.
