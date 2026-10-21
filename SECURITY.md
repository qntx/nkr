# Security

nosfry has not been independently audited.

## Reporting

Report vulnerabilities privately through
[GitHub security advisories](https://github.com/qntx/nosfry/security/advisories/new).
Do not open public issues that expose exploit details before a fix is
available.

## Scope

- Every client message is untrusted input. Events are size- and
  structure-checked before their BIP-340 signature is verified, and every
  queue, buffer, scan, and session set is bounded.
- The Prometheus endpoint (`relay.metrics`) is unauthenticated; keep it behind
  a reverse proxy or firewall.
- The NIP-86 management API relies on NIP-98 signatures and a 60-second window.
  A captured request can be replayed inside that window, so serve the relay
  over TLS.
- `relay.real_ip_header` must only be set behind a proxy that sets or appends
  the header; otherwise clients choose their own address.
