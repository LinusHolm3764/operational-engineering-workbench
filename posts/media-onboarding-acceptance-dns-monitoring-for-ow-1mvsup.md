# Media Onboarding Acceptance: DNS Monitoring for Ownership Checks and Mail Outcomes

A media publisher's onboarding deadline should not turn a successful DNS lookup into evidence that its mail was accepted. TL;DR: prove control of the intended domain before completing onboarding, then monitor record configuration, resolution, and mail outcomes as separate results. Choose who operates the zone before assigning the recovery work. A customer who controls the zone must participate in verification; a platform that operates a zone can automate its own changes, but those changes alone do not establish a customer's authority over a publication's domain.

This is an acceptance-testing problem. The useful question is which observation permits the next state transition, and which observation merely says that one dependency answered.

## An onboarding incident exercise

Consider a bounded exercise, not a reported production incident: a publisher is onboarding `daily.example`, a queued DNS check finishes, and the onboarding worker receives that completion twice. Meanwhile, an operator has replaced the pending ownership challenge. In this runbook, I would require the completion to identify the domain, the onboarding attempt, and the challenge generation before it can change state. That makes the exercise concrete without treating a hypothetical failure as a measured outage.

Hold the gate.

A concrete pitfall: generation 41 can finish after generation 42 has been issued. Those are example identifiers, not timing measurements. Don't let the older completion authorize the newer enrollment.

A completion for an earlier challenge must not approve the replacement challenge. A duplicate success must not trigger another welcome message. Even a current ownership result says nothing by itself about whether a receiving mail system accepted a message. These are proposed application invariants; they need explicit implementation and tests rather than an assumption that a DNS provider supplies them.

The postmortem question is precise: what evidence authorized completion? If the answer is only that a hostname resolved, the acceptance criterion needs repair. A runbook should preserve the observed result and its scope so the next operator can distinguish an obsolete check from a failed current check without repeating the entire onboarding sequence.

## What can DNS monitoring check records tell us about mail outcomes?

RFC 7489 describes DMARC as a mechanism that builds on SPF and DKIM authentication and identifier alignment. Its policy discovery procedure queries a DNS TXT record associated with the domain being evaluated. That procedure gives a useful boundary for monitoring: discovering a policy, evaluating authentication and alignment for a message, and observing the receiver's disposition are different operations.

A record exists. That is one observation.

It does not follow that an arbitrary message passes DMARC, or that a message passing DMARC reaches an inbox. RFC 7489 leaves message disposition to receiver policy, and its security considerations explicitly discuss the limits of relying on authenticated identifiers. Keep a successful authentication result separate from a delivery claim. An ownership token used by an onboarding application is another category entirely; DMARC policy discovery is not a specification for that token or for customer enrollment.

For this publisher, define three monitoring questions. Does the observed configuration satisfy the intended record policy? Does the required hostname resolve through the resolver path being tested? What evidence exists about the particular test message's acceptance or disposition? Record the observer and time with each answer. If no mail evidence exists, report it as unknown instead of borrowing a green status from the DNS check. This is a proposed monitoring contract, not a claim that one protocol returns all three answers.

The word accepted needs a definition in the runbook. For example, a test can require an explicit observation from a controlled receiving system. Specify what that observation means and which message it covers. Do not relabel a DMARC aggregate report as an immediate acknowledgment for a single onboarding email: RFC 7489 describes aggregate reporting across messages and reporting intervals, with distinct failure-reporting mechanisms and considerations.

## Decide who can repair the zone before scheduling checks

For a customer-owned zone, make the customer-facing instruction a stable challenge tied to the exact pending enrollment. Schedule observation of that challenge and show the last result. The operational trade-off is that the platform can observe a missing record while the customer retains control of the change that would resolve it. The proposed gate should therefore support a pending state without declaring a background verification job successful merely because it ran.

For a platform-owned zone, automate the permitted change and its verification, with the enrollment identity attached to both operations. This reduces the handoff for changes under platform control. It also creates a sharper authorization question: who may associate a publisher's identity with that domain? A platform's ability to write its own zone should not be the only evidence for that association. Decide the authorization rule independently of the DNS write.

| Decision | Customer-owned zone | Platform-owned zone |
| --- | --- | --- |
| Proposed change owner | Customer's DNS operator | Platform's authorized operator |
| Proposed pending-state action | Show the required challenge and latest observation | Inspect the authorized write and its observation |
| Proposed completion evidence | Current challenge proof plus required health results | Authorized association plus required health results |

Ownership determines the repair path. It shouldn't weaken the acceptance criterion.

This distinction also makes deployment review easier. Review a change to challenge issuance separately from a change to the health policy. Otherwise, a harmless adjustment to mail monitoring can accidentally change who gets approved for onboarding. Prefer a small, explicit policy version on an enrollment over a hidden dependency on whatever the newest worker happens to consider healthy.

## Make the completion path reject obsolete work

The following Go example models the decision after observations have been collected. It deliberately does not implement DNS token verification, message sending, or durable storage. Those adapters must produce the evidence named here. The generation value belongs to the enrollment and must change when its challenge or acceptance policy changes.

```go
package onboarding

import "errors"

type Enrollment struct {
    Domain     string
    Generation uint64
    Complete   bool
}

type Observation struct {
    Domain         string
    Generation     uint64
    OwnershipValid bool
    RecordsValid   bool
    ResolutionOK   bool
    MailAccepted   bool
}

func Apply(e Enrollment, o Observation) (Enrollment, bool, error) {
    if e.Domain == "" || e.Domain != o.Domain ||
        e.Generation != o.Generation {
        return e, false, errors.New("observation does not match enrollment")
    }
    if e.Complete {
        return e, false, nil
    }
    if !o.OwnershipValid || !o.RecordsValid ||
        !o.ResolutionOK || !o.MailAccepted {
        return e, false, nil
    }
    e.Complete = true
    return e, true, nil
}
```

The returned boolean means that this call proposes a transition. It is not permission to send a message before committing state. In production, compare the generation and completion state again inside the storage transaction; persist completion and an outbox entry atomically. Give the outbox event a stable enrollment identifier and generation, and make its consumer deduplicate that identity. Two workers can otherwise both read an incomplete enrollment and both believe they won. A pure function cannot settle that race.

Here, false includes missing evidence. The collector should preserve richer states such as pending, mismatch, and observation error for diagnosis. Collapse them only at the gate, where none authorizes completion. Validate the age of observations before calling this function using an explicit local freshness policy; matching generations alone do not prove freshness.

Test the transitions that could cause a false approval: an earlier generation returns after a replacement challenge, the same result arrives twice, ownership passes while mail evidence remains unknown, and concurrent workers attempt completion. Also test a resolver observation error separately from an observed record mismatch. These tests exercise the acceptance contract and recovery behavior. They do not need a claim about a commercial service's retry semantics.

## Keep health monitoring useful after acceptance

Make the verification job's execution status visible separately from its verdict. A completed job can report a mismatch. A job that never produced an observation leaves a different gap. For the proposed deployment, retain the enrollment generation, check category, observation time, and reason with each result; alert on missing expected observations as well as on explicit failures. Set the expected interval and escalation deadline from the publisher's onboarding requirement rather than inventing a universal DNS timeout.

Rechecking has an operational cost even without a price comparison. More frequent probes require more scheduling, result retention, and operator attention. Fewer probes delay evidence. Choose the interval alongside the permitted observation age, and make retries converge on the same pending enrollment instead of issuing a new challenge every time a worker runs.

DMARC reports can complement that view. RFC 7489 defines aggregate reports that describe authentication results and dispositions observed by reporting receivers. Use them for the scope they cover; an absence of reports should not be converted into proof that no mail problem exists. Reporting is an additional source of evidence with its own timing and participation boundaries.

There are limits to the gate shown above. A publisher onboarding only a web domain may have no mail requirement; its explicitly versioned policy should omit that prerequisite. An enrollment awaiting authorization cannot be approved merely because every network check is green. And ongoing health checks should produce current health state, rather than treating historical onboarding completion as evidence of present operation.

The durable rule is to attach every success to the claim it actually supports. Prove the domain association, evaluate the required health evidence, and commit completion once. Keep the zone owner visible so a failed observation has a repair path.

## Sources

References: the protocol claims above refer to RFC 7489. The onboarding policy, hypothetical exercise, and Go state transition are proposed application design.

- RFC 7489, Sections 3 and 4: authentication, alignment, and policy overview. https://datatracker.ietf.org/doc/html/rfc7489#section-3
- RFC 7489, Section 6.6: policy discovery and message evaluation. https://datatracker.ietf.org/doc/html/rfc7489#section-6.6
- RFC 7489, Section 7: aggregate and failure reporting. https://datatracker.ietf.org/doc/html/rfc7489#section-7
- RFC 7489, Section 10.4: identifier-based authentication limitations. https://datatracker.ietf.org/doc/html/rfc7489#section-10.4
