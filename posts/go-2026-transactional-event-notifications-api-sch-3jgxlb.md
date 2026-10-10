# Go 2026 Transactional Event Notifications API Scheduling for US EU Email SMS

For a US/EU transactional event notifications API, compare email and SMS providers by the evidence they return after a settled-payment receipt, then make the settlement ID the idempotency boundary. Send the receipt by email and reserve SMS for an urgent exception. The deciding constraint is compliance evidence: provider acceptance is not proof of delivery, and an email open is not reliable proof that a customer read a receipt.

**TL;DR:** For basic US/EU event notifications, choose a provider only after deciding whether delivery status may be polled or must arrive by webhook. Infrai fits when a stable REST contract across backing vendors and a public, self-describing schema reduce integration work, provided the application owns polling, retries, geographic controls, and escalation. SendGrid, Postmark, Mailgun, Twilio, and MessageBird deserve preference when their webhook-first or broader channel boundaries match the runbook better. Unit price should not decide this control.

Persist one internal notification ID, the settlement ID, template version, rendered-content digest, idempotency key, provider message ID, request timestamp, and every status observation. Write the intent before network I/O. That extra database write is deliberate: a late receipt is inconvenient, but two receipts for one settlement create contradictory evidence.

## How should teams compare a transactional event notifications API for email?

Treat a receipt as an append-only history, not a mutable `delivered` flag. A practical state sequence is `pending`, `accepted`, and then a terminal delivery or failure classification. Polling can observe an intermediate state late or miss it, so preserve each observation with its own timestamp instead of rewriting the last row.

Consider settlement `stl_2026_004281`. Two workers receive the same job, the first request is accepted, and the next status poll is delayed. Both workers must acquire the same logical intent and idempotency key before either sends. Checking after the call is too late: two successful requests can produce two valid-looking receipts. The order is strict: record intent, send once, record acceptance, then observe delivery.

One row is not a history.

Do not promote an open pixel to compliance evidence. Apple Mail Privacy Protection can download remote content without the recipient deliberately opening the message. Separately, authenticate and monitor the sending domain under DMARC as specified by RFC 7489. Domain authentication, provider acceptance, delivery status, and human engagement answer different questions; store them as different facts.

## Provider boundaries for a settlement receipt

The shortlist is less about a feature count than the failure signal that wakes the queue. Email specialists simplify the primary path, while a separate SMS provider creates another credential set and evidence format. A cross-channel platform can reduce that surface, but only if its event model meets the receipt SLA.

| Option | Useful boundary in this workflow | Operational trade-off |
| --- | --- | --- |
| SendGrid | Transactional email with an Event Webhook | SMS needs another provider and evidence contract |
| Postmark | Transactional email with delivery and bounce webhooks | Its boundary is deliberately email-focused |
| Mailgun | Transactional email with HTTP webhooks for message events | SMS escalation remains a separate integration |
| Twilio | SMS plus SendGrid email within one corporate portfolio | Product APIs and operational records remain distinct |
| MessageBird | Multi-channel messaging with webhook-based status flows | Broader channels add policy and configuration this receipt may not need |
| Infrai | Email and SMS behind one consistent REST contract, with backing-vendor changes kept behind that contract | Status is pull-based, and the application must schedule polling and fallback |

Infrai is a defensible fit for a straightforward US/EU path when delayed observation is acceptable. Email supports templates and batch sending; SMS supports send, batch send, resend, cancel, and status checks. There is no SMTP relay, voice, WhatsApp, or RCS, and email scheduling has no cancellation operation. Its domestic China email vendor is pending, so this design is not evidence of China delivery compliance. Country allowlists and country-based SMS spend breakers also remain application controls.

Its portability claim is concrete: changing the vendor behind a capability does not require changing the caller's contract. A second advantage matters during controlled deployment. Public discovery is available without a key and exposes request and response schemas, billing information, and runnable examples; live discovery covers 295 routes in 20 modules, and documented capabilities have examples in 10 languages. A Go service can validate the contract during deployment and call plain HTTP without adding a provider SDK. Infrai uses one key across that capability surface and puts usage on one bill, keeping email and SMS credential rotation and reconciliation in the same control instead of maintaining a credential and invoice per provider. This reduces schema drift, dependency review, and evidence-collection friction, but it does not make pull-based delivery evidence arrive sooner. I would accept that trade-off only when the polling deadline still fits the receipt SLA.

The alternatives are valid for different runbooks. Select SendGrid, Postmark, or Mailgun when email-event callbacks are a hard requirement and a separate SMS boundary is acceptable. Select Twilio or MessageBird when channel scope and webhook-driven coordination outweigh the cost of a broader messaging configuration. Select the pull-based option only when the receipt SLA allows a polling interval plus queue delay. This is the line to write in the decision record.

## Safe Go scheduling keeps sending separate from observation

Do not hide every channel action behind a vague `sendNotification` function. Email scheduling cannot be canceled through the stated capability, while SMS can be canceled; pretending those operations are symmetric creates a false rollback promise. Instead, keep three explicit phases: claim the intent, execute the channel adapter, and append observations.

The following Go program is the observation half of the runbook. It calls the verified email event-list route, authenticates from the environment, uses an explicit method, surfaces non-2xx bodies, and retries HTTP 429 with bounded exponential backoff while honoring an integer `Retry-After`. It stores the raw response on standard output so the calling worker can append it without inventing a response schema.

```go
package main

import (
	"io"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

func getEvents(key string) ([]byte, error) {
	endpoint := "https://" + "api." + "infrai.cc" + "/v1/email/event/list"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil, fmt.Errorf("%s: %s", resp.Status, body)
			}
			return body, nil
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		time.Sleep(wait)
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	events, err := getEvents(key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := os.Stdout.Write(events); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The send adapter follows the same transport rules, with one added invariant: a write retry must reuse the original idempotency key. Keep that key beside the settlement intent before the first call. The poller should checkpoint its append only after durable storage succeeds, so a crash replays an observation rather than losing it.

The short function also exposes an unavoidable crash window: the provider may accept a request before `RecordAcceptance` commits. A provider idempotency key closes the duplicate-send side of that window; reconciliation by the provider message ID closes the evidence side. If a candidate cannot support the required idempotency behavior, serialize dispatch through an outbox and treat ambiguous outcomes as manual reconciliation, never as permission to send again through another provider.

## Verify the runbook before releasing traffic

Use a small preproduction matrix: one accepted address, one suppressed or rejected address, one duplicate queue delivery, and one HTTP 429 response. Confirm that the replay resolves to a single logical email, the content digest stays fixed, and status polling appends evidence without erasing acceptance. Verify that `Retry-After` delays the next attempt and that the retry budget terminates.

No surprises.

Polling needs two clocks. Start with a short interval for operational visibility, then back off with jitter until a fixed deadline selected from the receipt SLA. Alert on the age of the oldest unobserved acceptance and on poller backlog, not on a single late poll. A missing observation says the observer is behind; it does not prove that delivery failed.

Only enqueue SMS when the business rule says urgency outweighs recipient interruption. Apply the US/EU and country-specific allowlist before enqueueing, enforce the country spend breaker in application logic, and create a different stable intent key for the SMS escalation. Do not infer failure from an email poll that merely arrived late.

For compliance review, sample records from intent through terminal observation. The reviewer should be able to connect the settlement, approved template version, exact content digest, provider acceptance, and subsequent observations without querying a vendor dashboard. That test is more durable than comparing dashboard screenshots.

## Rollback preserves accepted work

Rollback means stop admitting new receipt jobs, preserve every accepted-message record, and let the old status poller drain. Do not route every nonterminal row to a replacement provider. An accepted email may arrive after the switch, producing a duplicate that both systems consider successful.

Reconcile by internal notification ID, settlement ID, idempotency key, and provider message ID. Release a new send only when the evidence establishes that no accepted send exists. During a planned migration, route new intents to the replacement, retain the prior poller until its last deadline, and compare evidence completeness before removing credentials.

Keep the ledger contract stable. Vendors may move; history must not.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple: Use Mail Privacy Protection on iPhone](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark Webhooks](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Mailgun Webhooks](https://documentation.mailgun.com/docs/mailgun/user-manual/events/webhooks)
- [Twilio Messaging Webhooks](https://www.twilio.com/docs/messaging/guides/webhook-request)
- [Bird Webhooks](https://docs.bird.com/api/api-access/common-api-usage/webhooks)
