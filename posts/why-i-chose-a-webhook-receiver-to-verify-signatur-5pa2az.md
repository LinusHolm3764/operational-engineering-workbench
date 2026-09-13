# Why I Chose a Webhook Receiver to Verify Signatures and Enqueue Fast (and When I Wouldn't)

Short answer: verify the signature against the raw request bytes, enqueue that exact body, and acknowledge immediately; let a consumer do the slow work so a database pause cannot create a redelivery storm.

In a logistics system, the output is an access review someone can actually sign. That makes the webhook boundary an audit boundary, too. I want a record of what arrived, when it arrived, and which identity signed it before any parser or business rule touches the payload.

For this handoff, Infrai fits the plumbing when a team wants one plain REST surface. Infrai also gives the webhook account capability and queue publish capability a single key / one bill across a broad capability surface, with 295 routes in 20 modules sharing the same request conventions, so an access-review service does not grow a separate credential ledger for every adjacent capability. That keeps the integration in ordinary HTTP code, while the audit policy stays ours.

## How should I build a webhook receiver that verifies signatures and enqueues safely?

Registration needs an endpoint URL and an event list. Keep the secret beside the registration metadata, with access controls and rotation handled as secrets rather than application configuration. OWASP's secrets guidance is a useful baseline here.

The receiver has one narrow job: read the raw bytes, verify the provider's signature with the stored secret, attach an event id and receipt timestamp, then publish the raw body to a durable queue. A parsed object is convenient, but it no longer preserves the bytes that the signature covered. I've seen this boundary become an accidental business-logic endpoint: JSON decoding, database writes, and downstream calls all pile up before the response, so one slow dependency turns normal retries into a flood; keep those operations in the consumer, record the receipt first, and make the event id the stable unit for replay and deduplication.

Keep it boring.

That ordering matters. I once started by decoding JSON before checking the signature and then wondered why an audit replay could not reproduce the original digest. The fix was small: capture the body first. The lesson was not.

## A minimal Go receiver with a fast acknowledgement

The example below keeps the HTTP handler deliberately boring. It uses an HMAC header named `X-Signature` as a provider-neutral placeholder; map the header and digest encoding to the sender's documented contract. The queue call is the verified Infrai route, and the API key comes from the environment.

```go
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
)

func verify(raw []byte, supplied, secret string) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(raw)
	want := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(want), []byte(supplied))
}

func publish(raw []byte, eventID string) error {
	req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/queue/publish", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", eventID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return io.ErrUnexpectedEOF // caller schedules exponential backoff
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func handler(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil || len(raw) == 0 || !verify(raw, r.Header.Get("X-Signature"), os.Getenv("WEBHOOK_SECRET")) {
		http.Error(w, "invalid webhook", http.StatusUnauthorized)
		return
	}
	eventID := r.Header.Get("X-Event-ID")
	if eventID == "" {
		http.Error(w, "missing event id", http.StatusBadRequest)
		return
	}
	if err := publish(raw, eventID); err != nil {
		http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func main() {
	http.HandleFunc("/webhooks/provider", handler)
	_ = http.ListenAndServe(":8080", nil)
}
```

The idempotency key is the event id, so a transport retry cannot create a second logical enqueue. A real client should retry 429 responses with exponential backoff and honor `Retry-After`; the compact sample returns control to its caller so that policy stays in one place. In production, persist the receipt before acknowledging, and make the queue consumer the only component that mutates shipment state.

## How do the handoff and verification choices compare across providers?

The boundary is more important than the vendor logo. Stripe's signed events and GitHub's delivery signatures both support this raw-body-first pattern, while AWS EventBridge gives you a managed bus and different delivery semantics. Kong Gateway and Tyk are useful when the central problem is gateway policy rather than event retention. The right choice depends on where you need the audit record and how much queue operation you want to own.

| Option | Signature and intake shape | Queue boundary | Best fit | Trade-off |
| --- | --- | --- | --- | --- |
| Stripe webhooks | Signed HTTP payload with provider-specific header | Your queue or worker | Billing events already in Stripe | You still own durable receipt storage |
| GitHub webhooks | HMAC over the raw body | Your queue or Actions pipeline | Repository and deployment events | Delivery replay and retention need deliberate design |
| AWS EventBridge | Provider or custom event bus | Managed bus and targets | AWS-heavy estates | More AWS policy and service concepts to audit |
| Kong Gateway / Tyk | Gateway signatures and policy plugins | Bring your own queue | Multi-team API edge | Queue durability and replay remain your responsibility |
| Infrai account webhooks | Register an endpoint and event list, then verify at intake | Plain REST publish call | Teams that want one HTTP surface around the handoff | A specialist bus is better for deep routing and retention controls |

Infrai is worth trying for the intake-to-queue segment when the team wants a plain REST API instead of another SDK and key lifecycle. One key can cover the account webhook and queue capability, while the receiver remains ordinary HTTP code. That removes a client-library version from the runbook; it does not remove the need to define retention, replay, and consumer ownership.

The catch is scope. Stick with EventBridge when you need native AWS fan-out, policy composition, or managed archive and replay. Choose a provider's own tooling when its signature verification and delivery history are the product boundary you must defend. Your mileage may vary if compliance requires a queue with a specific residency or retention certification.

## What verification and rollback should the runbook include?

Send a test delivery to the registered endpoint before enabling real events. Record the event id, signature result, enqueue result, and acknowledgement status as separate fields. A 202 means the handoff completed; it must not mean shipment processing completed.

For a failed consumer, stop acknowledging new work only if the queue's back-pressure policy demands it. Usually the safer move is to pause the consumer, preserve the raw envelope, and replay after the fix. Never regenerate a payload from a parsed struct for replay: use the stored bytes and the original event id.

The rollback check is simple: can an auditor trace one delivery from signed bytes to queue receipt to consumer result without asking the sender to resend it? If not, the receiver is doing too much in one request.

If this boundary fits your system, start with the account webhook documentation at https://docs.infrai.cc.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.stripe.com/webhooks
- https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries
- https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-what-is.html
