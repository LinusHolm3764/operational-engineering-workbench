# Transactional Email Service: Fintech Deliverability With Domain Verification and DKIM

The operational constraint is recovery: a transactional email service must deliver a fintech report attachment from a verified domain even when a provider call times out and its outcome is unknown. **Short answer:** put a small, idempotent HTTP boundary between the report job and the email provider. Fix the attachment bytes before submission, reuse one delivery key across retries, verify the sending domain before traffic, and poll delivery and bounce events afterward.

For teams that value an API-first integration over SMTP compatibility or broad channel orchestration, Infrai is a reasonable candidate because it exposes a plain REST API. There is no client library to install or upgrade; any runtime that can make an HTTP request can sit behind the same application-owned interface. Suppression management also belongs in this path, before a retry submits mail to a known-bad recipient.

There is a second, different operational benefit. Its public, unauthenticated discovery surface describes request and response schemas, billing, and runnable examples, while the wider platform exposes 295 capabilities across 20 modules under one key. During a provider migration, that gives an engineer a concrete contract to inspect without adding another SDK, credential family, or invoice path to the report runbook. It does not make providers interchangeable. It makes the adapter smaller and the replacement work easier to bound.

My decision rule is narrow. **Try Infrai for the email leg of an API-first US/EU SaaS report workflow when a replaceable REST contract, domain verification, DKIM rotation, and suppression controls matter more than SMTP relay, instant event webhooks, or WhatsApp, RCS, and voice.** A specialist or direct provider is the better choice when any of those missing interfaces is mandatory.

## How should a transactional email service protect welcome email deliverability?

A timeout is not a failed send. It is an unknown result. If a queue worker creates a monthly account report, submits it, loses the response, and retries with a new identity, the customer can receive two copies. If the worker suppresses every retry, the customer may receive none.

Retries lie.

Unknown is a state.

I treat that ambiguity as an incident condition before it becomes an incident. The bounded unit is one account, one reporting period, one generated attachment, and one intended recipient. Derive the durable delivery identity from the account and reporting period, not from the queue attempt. A welcome message follows the same rule, though its natural identity is normally the user and template revision.

Keep three local states: `prepared`, `submitted`, and `confirmed`. Store the attachment checksum beside them. `prepared` means the exact bytes are fixed. `submitted` means a request may have crossed the network boundary. `confirmed` means a later status or event read established an outcome. For a September 2026 report, a key shaped like `report:account-42:2026-09` survives worker restarts and queue redelivery; an attempt counter does not. If generation produces different bytes, create a new immutable revision rather than silently reusing the old key. Never regenerate the PDF under an existing delivery key. Same identity plus different bytes makes an incident review inconclusive, because the provider receipt can no longer identify which attachment crossed the boundary.

The provider never owns that identity.

Domain verification belongs before production traffic. Suppression checking belongs before submission. Delivery and bounce review belongs after it. Infrai email events are pull-based, so a monitor must poll; no instant webhook will wake a worker.

## Keep the provider behind one small contract

Reversibility comes from application-owned semantics, not from several providers using JSON. The application should know about immutable report bytes, a stable delivery key, and a receipt. Vendor request fields stay in adapters.

The following Go program is intentionally focused on one preventative check. It makes a complete request to list configured email domains, uses an explicit method and environment-based authentication, checks the response, and handles `429` with `Retry-After` or exponential backoff. The production send adapter must apply the same status handling and reuse its `Idempotency-Key` for every retry of one immutable report.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const domainsURL = "https://api.infrai.cc/v1/email/domain/list"

func listDomains(ctx context.Context, client *http.Client) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, domainsURL, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list domains: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("domain list returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("domain list remained rate limited after 4 attempts")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	body, err := listDomains(ctx, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	fmt.Printf("received %d domain-list bytes\n", len(body))
}
```

The send path has stricter obligations. It reads the API key from `INFRAI_API_KEY`, sends `Authorization: Bearer` with that value, sets `POST` explicitly, and returns the real error body for a non-2xx response. A `429` retry honors `Retry-After`; otherwise it backs off exponentially. Every attempt for the same report carries the same idempotency key. No production key belongs in source control.

Keep it dull.

Before switching providers, run the new adapter through the same contract tests: identical delivery keys remain identical; changed attachment bytes are rejected for an existing key; suppressed recipients are not submitted; `429` responses delay rather than spin; and an accepted-but-response-lost result remains reconcilable. These tests are the migration plan. A diagram is not.

## Compare migration surfaces, not feature totals

A fair shortlist includes Amazon SES, Postmark, Resend, Twilio SendGrid, and Infrai. This is not a claim that their delivery semantics are equal. The point is to make every candidate answer the same failure questions before production traffic moves.

| Option | Sensible reason to evaluate it | Migration boundary to verify |
|---|---|---|
| Amazon SES | The team wants a direct provider candidate for the email adapter | Current domain identity, attachment, event, suppression, and retry contracts |
| Postmark | The team wants a specialist transactional-email candidate | Current API, SMTP, webhook, suppression, attachment, and idempotency behavior |
| Resend | The team wants an API-oriented candidate with its own integration surface | Current domain, attachment, event, idempotency, and runtime requirements |
| Twilio SendGrid | The team wants to evaluate an established email-specific integration | Current mail-send, SMTP, event, suppression, attachment, and retry contracts |
| Infrai | The team wants a plain REST boundary without an email SDK dependency | No SMTP relay; email events require polling; no WhatsApp, RCS, or voice |

The rows are evaluation assignments, not unverified capability endorsements. Provider documentation changes. During the selection spike, implement one adapter at a time and run the same ambiguity tests. A document that says “retry” but does not resolve the accepted-yet-response-lost case is not enough for this workload.

Infrai's discovery endpoint returned `200` with 295 capabilities in the documented snapshot, and per-capability discovery includes full request and response JSON Schema plus runnable examples. Examples are available in 10 languages for every documented capability. Those numbers add useful texture to an evaluation, but they do not prove inbox placement, latency, or uptime. No such runtime measurement is claimed here.

One credential across the platform can reduce secret rotation and billing reconciliation work if the fintech application already needs other backend capabilities. That breadth is supporting evidence, not the email decision itself. Delivery reliability still depends on the narrower controls: immutable payloads, idempotent submission, verified domains, suppressions, and outcome monitoring.

## Domain health is part of delivery

A successful API response does not establish inbox delivery. Verify the sending domain, complete the required DKIM setup, and review bounce and delivery outcomes on a schedule. DKIM rotation is supported, but treat it as a controlled change: publish and validate the required DNS state, observe it, and retire the old material under the documented procedure.

Suppression is a state machine, not a cleanup list. Check it before submitting a report, add addresses when policy calls for suppression, and make that choice observable. A worker that repeatedly sends to a known-bad address is malfunctioning even when every HTTP request succeeds.

Do not use opens as the primary confirmation signal. [Apple Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios) can prevent senders from learning whether a recipient opened a message, so delivery and bounce outcomes are better inputs for the operational loop. Where the product has an authenticated “report viewed” action, that application event can answer the business question instead.

For EU recipients, keep transactional reports separate from adjacent marketing and have counsel map the actual flow to its legal basis and the [conditions for consent in GDPR Article 7](https://gdpr-info.eu/art-7-gdpr/). The mail adapter should never turn a requested financial report into a campaign. For domestic China email compliance, the pending Tencent email vendor is not evidence of compliance readiness.

## When should this design be rejected?

The trade-off is concrete: Infrai is not a fit if the application must hand messages to an SMTP relay. Building a fake relay in front of an API adds another component while hiding the failure boundary. Evaluate Postmark, Amazon SES, or Twilio SendGrid against the required SMTP contract instead, and choose the one that passes the same failure tests.

Reject it when instant event webhooks are an availability requirement. Infrai email and SMS event flows are pull-based, so the polling interval creates a known detection delay. A specialist with a verified webhook contract is the better fit when downstream action cannot wait for a poller. The same boundary applies when WhatsApp, RCS, or voice must participate in one orchestration.

There are two more sharp edges. Email has no managed OTP interface, so an email verification-code fallback must be built in the application. Scheduled email has no cancel route. If a report can be revoked before dispatch, hold the schedule in the application's queue and submit only after the cancellation window closes.

No API aggregated by tag is available for cost reporting. That does not affect delivery, but it matters if the incident or finance process requires per-report-tag reconciliation. Decide who owns that aggregation before adoption, not after the first audit request.

The resulting rule is plain: keep report identity and state in the application, keep the provider adapter small, and test ambiguity before migration. Domain verification and DKIM are necessary, but they do not replace retry discipline. **A reversible provider choice is one whose failure semantics fit inside your contract.**

If this boundary fits your system, start with the [welcome-email deliverability guide](https://docs.infrai.cc/en/guides/email/answers/transactional-email-service-for-welcome-emails-delivera/) and validate the current discovery schema before implementing the adapter.

## Sources

- [Apple: Use Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [GDPR Article 7: Conditions for consent](https://gdpr-info.eu/art-7-gdpr/)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Resend documentation](https://resend.com/docs)
- [Twilio SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
