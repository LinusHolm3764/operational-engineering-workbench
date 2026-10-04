# Password Reset Paging: 4 Node.js Checks Before Adding SMS Backup

A page that says “password resets are failing” is already late. The least complex practical design is email-only with a short-lived, single-use token, plus delivery telemetry and a safe retry path. Add SMS backup only when measured email failures leave an unacceptable recovery gap and the organization can absorb a second identity attribute, consent rules, abuse controls, and another on-call dependency.

**Short answer:** for a fintech Node.js service, treat channel choice as an incident-prevention decision, not a price comparison. A successful API call is not a delivered reset. Instrument each transition, expire the credential independently of the message, and page on sustained user-impact signals. SMS can improve reach for some users, but it also adds SIM-swap exposure, phone-number lifecycle problems, and country-specific messaging obligations. Email-only is often the sound first release; optional SMS backup is a later, evidence-driven control.

This is the trace I want during an alert: request accepted, reset record committed, job claimed, message handed to the channel, outcome observed, token consumed. One correlation ID connects the steps without putting the token, email address, or phone number in logs.

## 1. What should have fired before support raised the alarm?

The on-call page usually starts with a symptom: customers requested resets but cannot sign in. Support may have timestamps and partially masked addresses. The responder needs to answer three questions quickly: did the application create a reset, did a worker attempt delivery, and did the channel report a terminal outcome?

Start with the denominator. Count accepted reset requests, then compare them with jobs that reach a terminal delivery state within a defined window. Also watch the age of the oldest ready job. A queue can report healthy process counts while one partition, tenant, or destination class quietly stalls.

Do not page on a single delayed message. Page on a sustained breach tied to user impact, such as a rising ratio of accepted requests without a terminal outcome over two evaluation windows. The exact window and ratio must come from normal traffic and the reset expiry chosen by the security team. An alert that fires after the token expires is useless; an alert that fires on every transient deferral trains the responder to ignore it.

The first instrumentation change is therefore a small state machine, not another vendor dashboard:

| State | Evidence to retain | Operational question |
|---|---|---|
| `accepted` | correlation ID, account surrogate, creation time | Did the API persist the request? |
| `claimed` | attempt number, worker ID, lease deadline | Is work moving, or is a lease stuck? |
| `submitted` | channel, provider-neutral message ID, timestamp | Did the channel accept custody? |
| `terminal` | delivered, deferred, bounced, rejected, or unknown | What did the latest evidence say? |
| `consumed` | reset ID, use time | Did delivery lead to recovery? |

“Submitted” is deliberately not “delivered.” SMTP acceptance and an SMS gateway acknowledgement are custody changes, not proof that a person saw the message. Preserve that distinction in metrics and incident language.

## 2. Trace the reset without leaking the credential

The reset token should never appear in a metric label, log field, queue name, or trace attribute. Tokens are secrets. Store only a one-way representation suitable for lookup, enforce one-time use atomically, and give the reset record its own expiry. Message retries must not extend that expiry.

Keep the public API response indistinguishable for existing and nonexistent accounts. That prevents the endpoint from becoming a convenient account-enumeration tool. Rate limits should consider account, source, and broader abuse patterns, while avoiding a rule so blunt that an attacker can lock a customer out merely by sending requests on the customer’s behalf. OWASP’s forgot-password guidance describes these controls and the need for a consistent response [1].

The following Go model is intentionally channel-neutral. A Node.js API can write the same outbox record in the database transaction that creates the reset, while a worker implemented in either runtime claims it. The important boundary is the data contract.

```go
package recovery

import "time"

type Channel string

const (
	Email Channel = "email"
	SMS   Channel = "sms"
)

type ResetDispatch struct {
	ResetID      string
	Correlation  string
	Channel      Channel
	Template     string
	Locale       string
	CreatedAt    time.Time
	ExpiresAt    time.Time
	Attempt      int
}

func (d ResetDispatch) Retryable(now time.Time, maxAttempts int) bool {
	return now.Before(d.ExpiresAt) && d.Attempt < maxAttempts
}
```

Notice what is absent: the raw token and destination. A worker can resolve encrypted contact data at send time under tighter access controls. The Mustache specification also recommends escaping variables by default; if templates use Mustache syntax, keep reset URLs in ordinary escaped variables rather than unescaped tags [2]. Template rendering should fail closed when required data is missing.

Idempotency matters at both edges. Claiming the same outbox row twice must not mint two independently valid credentials, and consuming a credential must be a compare-and-update operation that succeeds once. Retries happen. Design for them.

## 3. Choose email-only or SMS backup from failure evidence

Email-only has one contact attribute, one template family, and one delivery integration to operate. It is the better starting point when users reliably maintain email access, the measured terminal-failure rate is acceptable, and support has a verified recovery process for exceptional cases. Integration effort stays concentrated on the controls that every design needs: token safety, enumeration resistance, throttling, delivery events, and an auditable state transition.

SMS backup earns its place when real traces show that email failures materially block legitimate recovery and phone numbers are already verified and maintained for an appropriate purpose. Do not collect a phone number solely because a second channel sounds resilient. Numbers are reassigned, shared, ported, and sometimes controlled by an attacker. NIST SP 800-63B tells verifiers using the public switched telephone network to consider risks such as SIM change and number porting [3]. A fallback must not silently become the weaker path around stronger account controls.

Use a policy table during design review:

| Decision input | Favors email-only | May justify optional SMS backup |
|---|---|---|
| Observed delivery gap | Rare, short, recoverable | Sustained and tied to failed recoveries |
| Phone data | Not already verified | Verified, current, and governed |
| Abuse surface | One channel to defend | Separate limits and anomaly checks funded |
| Operations | One event model and runbook | Second integration has owners and drills |
| Regional handling | Email rules documented | SMS consent and routing rules also reviewed |

For US and EU users, classify the message by purpose before copying marketing conventions into it. The FTC explains that CAN-SPAM’s primary-purpose test distinguishes commercial content from transactional or relationship content; transactional messages must not contain false or misleading routing information [4]. In the EU, the GDPR requires data minimization, purpose limitation, storage limitation, and appropriate security for personal data [5]. Those principles affect whether a phone number should be collected and how long delivery evidence should remain identifiable. Legal review must map the actual message, recipients, and jurisdictions; an engineering article cannot make that classification for a specific business.

Cost belongs in the review, but it is not the main axis. Count the engineering cost of webhook verification, duplicate suppression, regional configuration, abuse response, data-subject handling, and on-call ownership. A low per-message quote does not retire any of those tasks.

## 4. Set the page so it catches harm without creating noise

Deploy the state machine before changing channels. Run synthetic resets against controlled inboxes, test expired and already-consumed credentials, inject worker termination after claim, and replay delivery callbacks. The invariant is straightforward: a retry may repeat transport work, but it must not create an additional usable reset or extend the security window.

Then establish a baseline by destination class and region without placing personal data in metric dimensions. Alert on a combination of backlog age and missing terminal outcomes. Ticket slow trends; page only when a responder has an immediate action, such as pausing a bad deployment, draining a stuck queue, or switching an approved route. The runbook should name the query, the expected states, the rollback owner, and the point at which security or support joins.

False positives have a real cost. If normal deferrals trigger a page, responders will spend attention proving that delivery recovered on its own, and repeated noise weakens the signal for the incident that actually approaches token expiry. Set thresholds from observed distributions, review them after incidents, and keep the threshold comfortably inside the useful response window. No magic percentage travels safely between systems.

The channel decision follows from that evidence. **Keep email-only while it meets the recovery objective; add SMS backup only for a demonstrated gap with explicit security, compliance, and operational ownership.** The best fallback is the one whose failure can be seen before the customer reports it.

## Further reading

1. OWASP, “Forgot Password Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
2. Mustache template syntax manual: https://mustache.github.io/mustache.5.html
3. NIST SP 800-63B, “Authentication and Lifecycle Management”: https://pages.nist.gov/800-63-4/sp800-63b.html
4. FTC, “CAN-SPAM Act: A Compliance Guide for Business”: https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
5. Regulation (EU) 2016/679, Article 5: https://eur-lex.europa.eu/eli/reg/2016/679/oj
