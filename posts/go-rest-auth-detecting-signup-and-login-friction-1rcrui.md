# Go REST Auth: Detecting Signup and Login Friction Before Lockout Storms

A B2B SaaS needs email signup and password login that legitimate users can complete, with sessions that stop working when security policy requires it. Start with a small REST boundary and explicit session rules. Consider this runbook scenario: the page says login failures are rising, while the dashboard shows successful responses from the authentication service. The on-call sees users returning to the sign-in screen, repeated attempts, and an apparently healthy dependency. Those observations are a diagnostic example, not a reported incident.

**Short answer:** Evaluate a plain REST authentication alternative by whether you can distinguish rejected credentials, throttling, and unusable sessions before users repeatedly retry. Keep public errors generic, expose bounded internal outcomes, and test session invalidation separately from login success. For this workload, the useful decision axis is session security versus friction; a successful request alone cannot settle it.

A search for an Auth0, Clerk, or Supabase auth alternative may start with the cheapest plain REST API for signup and login. The engineering question is what happens between an accepted password and usable application access. Apply the same workflow tests to every candidate; this note makes no claims about differences between those products.

## What should have alerted before users started retrying?

Work backward from the page. A rejected password can mean an ordinary typo or an attack. A session that fails immediately after an accepted login suggests a different investigation. Combining them into one failure count makes the response ambiguous: tightening controls might increase legitimate-user friction, while relaxing controls might weaken protection.

Separate the signals.

For the illustrative incident above, the earlier signal to look for is an increase in rejected first authenticated requests following accepted logins. That observation describes the application boundary, where a browser actually presents its session. Authentication-service availability cannot establish that the session reached the browser, that the application accepted it, or that a subsequent security decision preserved it. Instrument each transition before choosing an alert threshold.

The OWASP Authentication Cheat Sheet recommends generic authentication responses to reduce account enumeration, login throttling against automated attacks, and session invalidation and rotation after reauthentication. These requirements create an operational distinction: the response visible to a client should reveal less than the internal evidence available to an authorized responder. Keep that distinction deliberate. Don't expose an account-existence reason to make a dashboard easier to build.

For signup, observe acceptance and subsequent ability to sign in as separate events. A user account being created does not, by itself, establish a usable session. If the application requires email verification, make that state explicit in the workflow and in internal outcome reporting; do not classify every unfinished verification as an authentication-service outage.

## Change the instrumentation at the decision boundary

Record the result where the decision is made. The login handler can report credential acceptance; the session validator can report whether the presented session is valid. Neither should guess the other's result. Use a fixed vocabulary, and keep email addresses, passwords, cookies, and session tokens out of metric labels. A bounded reason is useful. A credential isn't.

The following Go example is an instrumentation component for the application, not an authentication implementation. It exposes five fixed counters through a snapshot. Wire calls into the actual decision branches, export snapshots through the existing metrics collector, and aggregate them before calculating fleet-wide rates. The counters are process-local and reset on restart, so a collector must handle resets.

```go
package authsignal

import "sync/atomic"

type Counters struct {
    accepted       atomic.Uint64
    rejected       atomic.Uint64
    throttled      atomic.Uint64
    sessionValid   atomic.Uint64
    sessionInvalid atomic.Uint64
}

func (c *Counters) LoginAccepted() { c.accepted.Add(1) }
func (c *Counters) LoginRejected() { c.rejected.Add(1) }
func (c *Counters) LoginThrottled() { c.throttled.Add(1) }

func (c *Counters) SessionChecked(valid bool) {
    if valid {
        c.sessionValid.Add(1)
        return
    }
    c.sessionInvalid.Add(1)
}

func (c *Counters) Snapshot() map[string]uint64 {
    return map[string]uint64{
        "login_accepted":  c.accepted.Load(),
        "login_rejected":  c.rejected.Load(),
        "login_throttled": c.throttled.Load(),
        "session_valid":   c.sessionValid.Load(),
        "session_invalid": c.sessionInvalid.Load(),
    }
}
```

These counters answer broad operational questions. They do not identify the first session check after a particular login. For that narrower measurement, emit a separate transition event when the application can establish the relationship, using a restricted correlation mechanism rather than a session token. Count each transition once. If that linkage is unavailable, label the measurement as overall session rejection; do not present it as proof that freshly issued sessions are failing. The snapshot also loads counters individually, so it is not a transactionally consistent view of simultaneous events.

Retries complicate the denominator. Ten requests can represent ten people, one browser retrying, or an automated client. Keep request counts, authentication attempts, and completed workflows distinct. Where workflow events pass through a queue, use stable event identifiers and idempotent consumption so redelivery does not inflate completed-login counts. Replaying telemetry must not mint a session or rerun a password check.

This adds a little instrumentation work. It buys a much clearer response when the page fires.

## Preserve security controls while investigating friction

Begin triage with the branch that changed. If throttling rose, examine attempt patterns and the configured policy before changing it. If credential rejection rose, check the relevant failure categories internally while preserving generic public responses. If session rejection rose after an accepted login, inspect session delivery and validation, and compare the timing with deployments or intentional invalidation. Record evidence before changing a control.

Don't turn off throttling just to clear a dashboard.

OWASP describes account lockout as a trade-off: it can resist password guessing, but attackers can also exploit it to deny access to legitimate users. A plain REST interface does not remove that trade-off. The service must define the controls, and the application must present a recoverable experience without disclosing whether an account exists. Choosing a shorter lockout or a different threshold requires a stated threat model and observed traffic, rather than a universal number copied from an example.

Session policy has the same tension. OWASP recommends reauthentication for sensitive operations and after risk events, including account recovery. That creates intentional interruptions. Track them separately from unexplained session failures, but retain evidence that invalidation actually worked. Otherwise, an alert tuned only for uninterrupted access can reward a security regression.

A repeated login request also deserves care. A client timeout does not prove that the server rejected the credentials. Define retry behavior and prevent retries from producing repeated downstream side effects, such as duplicate welcome messages. Never let an idempotency key become an alternate credential: reuse of a request identifier must not authorize access or extend a session.

## How should a plain REST auth alternative handle signup and login retries?

Use the same small acceptance suite for any candidate implementation:

- Create an account, attempt a valid login, and present the resulting session to the application.
- Exercise the application's logout and recovery policy, including rejection of sessions that should have been invalidated.
- Check wrong passwords, throttled attempts, and expired sessions against explicit expected outcomes.

Expected results belong in the test definition. They should not be inferred from whichever behavior a candidate happens to return.

Public responses need their own checks. Compare the response content and observable behavior for nonexistent accounts and incorrect passwords against the enumeration protections described by OWASP. Internal diagnostic detail must remain internal. Also test that credentials and session material do not appear in telemetry. A useful trace is a poor trade if it creates another place to steal access.

During deployment, compare credential acceptance and application session validation across the rollout. A change that improves one counter while degrading the next transition has not improved the signup-to-access journey. Preserve a rollback path, but check whether rolling back would undo a required invalidation or restore acceptance of a session that security policy has revoked.

The selection rule is operational: require documented session and reauthentication behavior, observable outcomes, and a testable failure boundary. A concise REST interface can reduce integration surface, while pushing more workflow and incident-response responsibility onto the application team. Include that work in the decision. A headline price cannot tell you who will diagnose a user locked out during account recovery.

## Set a threshold that earns the interruption

Close the runbook with an alert rule the team can defend. For the first-session transition metric, define the eligible population, the observation window, a minimum count, and the rejection fraction that warrants paging. Keep intentional security invalidations visible through a separate signal. The exact thresholds require a baseline from this application; none is established here.

As arithmetic only, one rejection among two eligible transitions is 50%, while one among 200 is 0.5%. Neither percentage identifies the cause. A minimum sample avoids treating those windows as equivalent evidence, but waiting for that sample also delays detection in a quiet B2B tenant. Use synthetic workflow checks or another independently defined signal to cover that gap, with test traffic identified separately from real-user traffic.

Set the threshold too low and ordinary typos, planned invalidations, or a small traffic window can consume the on-call's attention. Set it too high and legitimate users absorb the delay. Repeated false positives also create pressure to relax controls simply to silence the page. Review each alert for the action it enabled: preserve the security boundary, identify the failed transition, and restore access under the intended session policy.

## References

The authentication guidance cited here covers generic error responses, login throttling and lockout trade-offs, reauthentication, and session handling after reauthentication. The counters and alert workflow are proposed application design, not measured results.

## Further reading

- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
