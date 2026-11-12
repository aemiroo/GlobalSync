# Security policy

## Scope and security claims

GlobalSync is experimental software. It is not currently safe to expose a peer port: this release does not open one, and `network.enabled` must remain `false`.

No plugin can guarantee that it is impossible to backdoor. GlobalSync aims to make unauthorized behavior difficult to introduce, constrain the impact of a compromised node, and make releases auditable. A clean build or a passing test suite is not proof that source code is benign.

## Current trust boundaries

- The Minecraft server process and its administrators are trusted. A server operator, host administrator, JVM agent, or another plugin with equivalent process access can alter GlobalSync behavior or read its secrets.
- The local journal is trusted only to the extent that the server account and filesystem are protected. SQLite durability is not tamper-proof storage.
- Peer certificates authenticate possession of a private key. The configured trust store and per-peer SHA-256 fingerprint allowlist must both authorize a peer.
- A valid peer must not be treated as globally authoritative. Future protocol handlers must validate message type, schema, world allowlist, origin identity, sequence, size, and authorization independently.
- A compromised regional node must not be allowed to execute commands, upload code, select arbitrary file paths, or change another node's configuration.

## Current safeguards

- Peer networking is disabled by default and no listener is started in this milestone.
- The transport foundation is TLS 1.3 only, supports mandatory client certificates, and provides explicit leaf-certificate fingerprint verification.
- Frames are length-prefixed and bounded before payload allocation.
- TLS private keys and store passwords must remain outside source control. Supply passwords through the configured environment variables.
- The plugin has no supported remote command or arbitrary code execution feature.

## Required controls before enabling networking

Do not enable peer networking until all of these exist and are reviewed:

1. Mutual TLS with certificate-chain validation, hostname/identity checks where applicable, and an explicit peer ID-to-certificate fingerprint allowlist.
2. No trust-all trust managers, disabled certificate checks, plaintext fallback, shared default passwords, or secret values in logs.
3. A strict message allowlist and schema validation. Reject unknown message types, unsupported protocol versions, oversized fields, invalid IDs, invalid world names, and unauthorized origins.
4. Per-peer rate limits, connection limits, read/write timeouts, bounded queues, and backpressure.
5. Durable receiver-side deduplication and acknowledgements only after commit.
6. No remote console, command dispatch, class loading, scripting, plugin installation, file access, or configuration mutation through the replication protocol.
7. A safe failure mode: malformed or unauthorized messages are rejected; they never trigger world changes.
8. Integration tests for invalid certificates, untrusted peers, wrong fingerprints, replay, malformed frames, oversized messages, and disconnects.
9. A documented certificate rotation and revocation procedure.

## Release and repository hygiene

- Review every source and dependency change before merging.
- Protect the default branch with required review and CI checks. Do not allow unreviewed direct pushes.
- Pin GitHub Actions to immutable commit SHAs in release workflows and grant workflows only the permissions they need.
- Generate an SBOM and scan dependencies before publishing a release. Review transitive dependencies as well as direct dependencies.
- Publish release artifacts from tagged commits and retain build provenance/attestations where available.
- Verify release checksums and provenance before installing a jar. Never install a jar merely because it has the expected filename.
- Keep private keys, trust-store passwords, tokens, and production configuration out of Git. Rotate any secret that was committed or exposed.
- Build from a clean checkout with the documented Java and Maven versions. Compare the reviewed source commit with the release tag.

## Reporting a vulnerability

Do not publish exploit details or secrets in a public issue. Use GitHub's private vulnerability reporting for this repository if enabled, or contact the repository maintainer privately with the affected version, impact, and a reproducible description.

## Operator checklist

- Run GlobalSync under a dedicated, unprivileged server account.
- Restrict filesystem permissions on the plugin data directory and TLS stores.
- Restrict inbound and outbound firewall rules to known peer addresses.
- Keep the Paper server, Java runtime, operating system, and other plugins patched.
- Audit the full plugin set; another plugin running in the same JVM can potentially access GlobalSync data and memory.
- Back up the journal and TLS material securely, and test restoration.
- Leave peer networking disabled until the controls above have been implemented and tested.
