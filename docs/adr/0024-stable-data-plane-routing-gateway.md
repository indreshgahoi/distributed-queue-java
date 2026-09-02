# ADR 0024: Introduce a Stable Data-Plane Routing Gateway

## Status

Accepted for v0.25.0.

## Context

Queue clients currently address the node hosting partition zero. Node location
is control-plane state and will change when ownership transfer is introduced.
Exposing it makes clients responsible for placement discovery and prevents a
stable SQS-like queue endpoint.

A route is also immediately stale after observation. The gateway must not be
mistaken for the authority that grants storage access, and automatic mutation
retry after a transport failure could duplicate a durably committed publish.

## Decision

Add an independently deployable `queue-gateway` using domain, application-port,
and adapter boundaries. For every customer operation it:

1. asks metadata-service for the authoritative READY route;
2. forwards the request exactly once to the selected node;
3. returns the node status, content type, body, and public Location;
4. never retries or resolves another route after an ambiguous node call.

Metadata route resolution uses one PostgreSQL statement and returns a route
only when current queue generation, ACTIVE lifecycle, partition placement,
unexpired node registration, READY runtime status, placement epoch, and
registration epoch agree.

The queue node remains the final admission authority. A metadata route is a
fenced discovery observation, not an ownership token.

HTTP calls have bounded connect and request timeouts. Metadata unavailability
returns 503, absence of a READY route returns 503, an unknown queue returns
404, and an unreachable selected node returns 502. Downstream application
responses such as 204, 413, 429, and 503 are preserved.

## Consequences

- customers can use one stable gateway address;
- metadata lookup is currently paid on every operation;
- metadata availability is temporarily on the data-plane request path;
- a route can become stale between lookup and forwarding, so the node still
  rejects non-serviceable runtimes;
- failed forwarding can be ambiguous and is surfaced without automatic retry;
- no route cache, load balancing, ownership transfer, or replication is added;
- node endpoints are trusted metadata and HTTP(S)-validated, but internal API
  authentication remains deferred for this local learning deployment.
