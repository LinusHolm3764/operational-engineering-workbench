# DNS TTL Selection Across 3 Scheduled Stages of Planned Customer Changes

A scheduled domain change can reach its appointed time while cached answers still reflect the previous configuration. TL;DR: choose a short DNS TTL for a bounded change window and a longer TTL for stable operation, but lower it early enough for answers carrying the previous TTL to expire. Start that waiting period from verified publication of the lower TTL. A scheduled write alone is insufficient evidence.

For a developer tool accepting customer domains, the operational decision is about drift between intended records and published records. Track three stages separately: publication of the lower TTL, the cache waiting period, and publication of the new destination. Treat the cutover job as a reconciliation attempt with a readiness condition, rather than an unconditional timer.

## A missed prerequisite can look like a successful cutover

Consider a planning exercise, not a reported production incident. A customer points `build.customer.example` at the product. The approved plan starts with a TTL of 3,600 seconds, temporarily lowers it to 300 seconds, then changes the destination at a scheduled time. These are example settings, not measurements or universal defaults.

The lowering job is delayed.

The destination job runs on schedule.

Both jobs can eventually report successful writes while the intended sequence has already been violated. A recursive resolver that obtained the old destination with the previous TTL can retain that answer for its remaining lifetime. Lowering the authoritative TTL does not rewrite the TTL on an answer already held elsewhere. RFC 1035 defines TTL as the interval for which a resource record may be cached; RFC 2181 clarifies TTL handling and consistent TTLs within a record set.

The invariant is small: **do not authorize the planned destination change until the previously published TTL has elapsed after verified publication of the lower TTL.** This establishes a conservative waiting condition for ordinary positive caching. It does not prove every client has stopped using the old destination.

Keep that distinction in the incident record. “The change request succeeded” describes a write. “The preparation condition was satisfied” describes a different piece of evidence. Combining them into one green status hides the scheduling failure that matters to the customer.

## How should DNS TTL selection account for delayed changes?

Begin after checking the relevant authoritative servers for the expected record set and lower TTL. Persist the observation time, the previous TTL, and the configuration generation together. If publication differs across authoritative servers, preparation remains incomplete. The conservative start is the time by which all checked authoritative servers have been verified, rather than the time the first update request was sent.

For the example, verification at 12:00 with a previous TTL of 3,600 seconds gives an earliest planned cutover of 13:00. A request submitted at 11:45 does not move that boundary backward. A resolver could have fetched the old answer immediately before the lower TTL became available. Waiting one previous TTL accounts for that possibility under ordinary cache expiry rules.

The new 300-second TTL governs answers fetched after publication. It cannot shorten an earlier 3,600-second cache entry.

That is the specific trap behind lowering the TTL five minutes before a change and expecting a five-minute transition.

Check every dependency that will change. If the customer-facing name points through an alias, the alias record and the destination's address records are separate record sets with their own TTLs. A preparation check covering one name cannot establish readiness for a different record set. Make the plan explicit about which records the customer owns and which records the product controls; otherwise the scheduler can claim readiness using observations from the wrong zone.

An absent record needs separate handling. RFC 2308 defines negative caching, including the role of the zone's SOA data in setting the negative response TTL. Creating a name after clients have received a negative answer does not make that cached answer disappear. A positive-record TTL waiting calculation is inadequate for that case.

## Make duplicate and delayed jobs recheck the same evidence

The preventative path should validate readiness when the cutover worker executes. Its enqueue time is historical context. Its current configuration generation and publication evidence decide whether it may proceed.

The following Go example focuses on that gate. The verifier supplying this data must check record content as well as TTL, and `PublishedAt` must describe verification of the relevant authoritative set. `PreviousTTL` is the TTL before preparation began. It must not be replaced with the newly lowered value during a retry.

```go
package cutover

import (
	"errors"
	"time"
)

type Preparation struct {
	Generation          uint64
	VerifiedGeneration  uint64
	PublishedAt         time.Time
	PreviousTTL         time.Duration
	AuthoritiesAgree    bool
}

func ReadyAt(p Preparation, now time.Time) (time.Time, error) {
	if p.Generation != p.VerifiedGeneration {
		return time.Time{}, errors.New("publication evidence is outdated")
	}
	if !p.AuthoritiesAgree || p.PublishedAt.IsZero() {
		return time.Time{}, errors.New("publication is unverified")
	}
	if p.PreviousTTL <= 0 {
		return time.Time{}, errors.New("positive previous TTL required")
	}
	if now.Before(p.PublishedAt) {
		return time.Time{}, errors.New("publication time is in the future")
	}
	ready := p.PublishedAt.Add(p.PreviousTTL)
	if now.Before(ready) {
		return ready, errors.New("cache waiting period is incomplete")
	}
	return ready, nil
}
```

This function is a gate, not an entire controller. Immediately before writing, use a generation check against durable state so that a concurrent customer edit cannot authorize a stale job. Tie the idempotency key to the customer domain, record set, and intended generation. Repeated delivery should reconcile the same destination; it should not reset the preparation clock or advance an unrelated plan. If the current records disagree with the verified preparation, stop and obtain fresh evidence. For example, a queued change to one destination must lose authorization when the customer selects a different destination, even if the original preparation deadline has passed. Keep that check and the state transition coordinated so a worker cannot validate one generation and then act on another. Persisting a deadline without its generation leaves precisely that ambiguity for the next retry.

Test the boundary.

One instant before readiness must defer; equality may proceed. Also test an outdated generation, missing verification, and a duplicate delivery after the intended destination is already published. A delayed preparation job should shift readiness forward, even when that means missing the original appointment. That is an explicit scheduling trade-off: preserve the cache preparation condition and tell the customer the schedule changed.

## Choose TTLs around the transition you can operate

Keeping a short TTL continuously avoids a separate lowering phase, but it also makes cached answers expire sooner. For repeatedly requested names, that creates more opportunities for cache misses and authoritative queries. A longer TTL allows longer cache reuse while making an unprepared destination change slower to age out. Neither setting guarantees availability. Select the stable value around acceptable change latency and the DNS service's operational capacity, then define a temporary value appropriate to the planned transition.

Pre-lowering adds coordination work. Someone must own publication verification, the waiting deadline, and restoration of the steady-state TTL. For an infrequently changed customer domain, that work can be reasonable. For a domain whose destination changes continuously, repeatedly staging preparation may be the wrong operating model; keep DNS stable and consider whether routing decisions belong behind that stable name. The architecture decision depends on who controls the destination and what the application can route.

After the change, retain the short TTL while validating the new destination and deciding whether to revert. Raising it immediately creates longer-lived cache entries for a destination still under evaluation. A reversal is another publication event: answers already cached for the new destination remain subject to their cache behavior. Preserve the previous destination's ability to serve requests during the planned overlap when the system permits it.

Observe the sequence directly. Record the intended generation, verified generation, verification timestamp, readiness deadline, and destination publication result. Measure how long preparation remains unverified and how often a cutover arrives before readiness. Sample recursive answers as additional evidence, with resolver identity and observation time attached. A successful sample establishes what that resolver returned at that moment; it does not establish universal convergence.

Keep the customer-facing explanation short: preparation verified, waiting until a named time, or destination changed and validation underway. Operators need the record-level evidence.

Customers need an honest account of what happens next.

## Where the waiting rule stops helping

TTL expiry is not a universal traffic deadline. RFC 8767 permits recursive resolvers to serve stale data under specified conditions. Application behavior can add another boundary: DNS evidence alone does not demonstrate that existing connections have moved. Keep DNS readiness, destination health, and observed request traffic as separate checks.

Emergency recovery also changes the decision. If the current destination cannot serve requests, waiting to satisfy a preparation policy may be unacceptable. Publish the recovery change according to the incident decision and state the remaining uncertainty about cached answers. A TTL selected during an incident cannot retroactively shorten existing cache entries.

For planned customer-domain changes, the durable lesson is narrower and actionable: lower the TTL before changing the destination, wait using the previous TTL and verified publication time, and make every job delivery revalidate intent. Restore the stable TTL after destination validation. Treat disagreement as drift requiring investigation, rather than declaring the schedule successful because a worker ran.

## Sources

References for DNS caching, record-set consistency, negative answers, and stale-answer behavior:

- RFC 1035, Domain Names — Implementation and Specification, especially resource-record TTL semantics: https://datatracker.ietf.org/doc/html/rfc1035
- RFC 2181, Clarifications to the DNS Specification, especially section 5 on record sets and TTLs: https://datatracker.ietf.org/doc/html/rfc2181
- RFC 2308, Negative Caching of DNS Queries: https://datatracker.ietf.org/doc/html/rfc2308
- RFC 8767, Serving Stale Data to Improve DNS Resiliency: https://datatracker.ietf.org/doc/html/rfc8767
