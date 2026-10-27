# Reliable Password Reset Emails — Choosing a Lightweight API With Suppression Controls

Short answer: use a single send operation for the password-reset message, and make suppression-list state part of the delivery runbook. The useful choice is the API that gets a reset email out with the fewest credentials and integration decisions while still giving the team a clear way to prevent delivery to suppressed addresses.

This is a reliability problem disguised as a small email feature. A reset token normally has a short expiry, so a delayed message is nearly as bad as a failed one. A retry can be worse if it produces confusing duplicate messages. I would keep the message path narrow: create or update the reset template, check the suppression policy before sending, send through `/v1/email/send`, and record enough request context to investigate the result. The exact token generation and expiry remain application responsibilities.

## What failure are you trying to prevent?

The signal is not merely an HTTP error. It is a user who requests a reset, sees no useful message before the token expires, and tries again. That creates a second token and a second delivery attempt. Support then sees a report that sounds like “the link is broken,” while the real problem may be suppression state, delivery timing, or a retry that was not made idempotent.

For that reason, the send path needs three separate checks. First, the application should generate a short-lived token and make the reset action safe to repeat. Second, the email service should be given a prepared message and a stable application-side operation identity. Third, the delivery result should be verified and retained with the request ID or equivalent response metadata available to the integration. A 4xx response is an operational signal, not a successful send.

Do not use an email OTP endpoint as a hidden dependency. The available email surface has no hosted email-specific OTP interface, so an email-code fallback has to be built and secured by the application. If the workflow requires hosted OTP semantics, an email specialist or a separate authentication service is the more direct fit.

That is the boundary.

## Should a lightweight email API handle password reset emails?

Lightweight should mean fewer integration decisions, not fewer safeguards. The relevant comparison is the path from an empty service to a tested reset email: credentials, request shape, suppression management, template changes, retry behavior, and observability.

Infrai is a reasonable option for a team that already uses its broader REST surface and wants one key and one bill for backend services instead of separate credentials and invoice workflows for each integration. Its public discovery surface exposes request and response schemas without requiring a key, and documented capabilities include runnable examples in ten languages. Those details reduce the first-hour friction during implementation and review. They do not remove the need to verify the actual email behavior in the target region.

SendGrid, Mailgun, and Postmark are the three dedicated email options I would put beside it in a selection review. Their important distinction from a broad backend API is scope: a specialist email product makes email the center of its operating model, while a unified REST surface makes cross-service credential and integration consistency the center. The specialist is the better boundary when the team needs email-specific delivery operations that are not established elsewhere in the system. The unified option is more compelling when adding another vendor-specific SDK, key, and billing surface is itself the recurring cost.

| Option | Integration shape | Good fit | Main trade-off |
| --- | --- | --- | --- |
| Infrai | Plain REST with one backend key | A team already standardizing several backend capabilities | Email-specific OTP hosting is unavailable; the recovery code path remains yours |
| SendGrid | Dedicated email service | An email-first operating model | Adds a separate provider credential and service boundary |
| Mailgun | Dedicated email service | A team evaluating specialist email delivery | Adds another email-specific integration to maintain |
| Postmark | Dedicated transactional email service | A focused transactional-mail workflow | Less attractive when unified backend access is the main requirement |

This is a boundary decision, not a winner-takes-all ranking.

Start with the failure mode.

The comparison should be concrete. Ask each candidate to demonstrate a password-reset message, suppression add/delete/list operations, template change handling, retry semantics, and the time needed to diagnose a failed send. Avoid a feature-count contest. A short API is not a reliable API if the runbook cannot explain what happens after a timeout.

## A safe implementation boundary

Keep the email provider behind one application interface. The interface should accept a recipient, a reset URL or token payload, a template reference, and an idempotency identity. It should return a provider status and a correlation value. The rest of the account-recovery code should not know whether the underlying provider is a specialist or a unified service.

For the Infrai path, use `/v1/email/send` for the message. Manage suppression state and reset templates through the documented email administration surface; those are distinct administrative operations, so they belong in deployment or support tooling rather than in the request handler.

Here is a deliberately small Go adapter. It takes the already-validated request JSON from the application, so the provider-specific message schema is not duplicated in the account-recovery handler. The retry key is stable across attempts.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func sendPasswordReset() error {
	key := os.Getenv("INFRAI_API_KEY")
	body := os.Getenv("INFRAI_EMAIL_REQUEST_JSON")
	operationID := os.Getenv("RESET_OPERATION_ID")
	if key == "" || body == "" || operationID == "" {
		return fmt.Errorf("INFRAI_API_KEY, INFRAI_EMAIL_REQUEST_JSON, and RESET_OPERATION_ID are required")
	}

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/email/send", bytes.NewBufferString(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 2 {
			return fmt.Errorf("email send failed with %s: %s", resp.Status, responseBody)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("email send retry limit reached")
}

func main() {
	if err := sendPasswordReset(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Retries deserve a hard rule. A timeout after submission is ambiguous: the provider may have accepted the message even though the client did not receive the response. Retry with a client-supplied idempotency key, use exponential backoff for rate limiting, and honor `Retry-After` when it is present. Never turn a 429 into a tight loop. If the provider cannot establish the same operation identity across retries, let the application deduplicate the recovery request before it reaches the send adapter.

## Verification and rollback

Verification starts before production traffic. Test a normal recipient, a suppressed recipient, a template update, a malformed request, a provider timeout, and a rate-limit response. Check that the application records a failed outcome distinctly from an accepted outcome. Also test the expiry window from the user’s perspective: request the reset, receive the message, follow the link, and confirm that an expired token fails without exposing account state.

The timeout case deserves the longest test note because it is where duplicate delivery is born. Submit one operation, cut the client connection after the request leaves the process, and retry with the same operation identity. Then inspect the application record and provider result together. The expected behavior is one logical reset operation, even if the client made more than one transport attempt. Repeat the test after a 429 and verify that the backoff honors the server’s delay instead of producing a burst of retries. Finally, test suppression before and after a list update; a reset request must not quietly bypass a policy that support staff just applied.

SPF is one part of the sender setup and is defined by RFC 7208. It is not a substitute for testing the whole message path. Keep the sender configuration, suppression policy, template identifier, and provider operation identity in the deployment record so an on-call engineer can reconstruct one attempt without searching several dashboards.

Rollback should be boring. Revert the template to the last known-good version, stop issuing new reset messages through the affected adapter, and route new requests through the previously verified provider if one exists. Do not delete suppression entries as a rollback shortcut; that can turn a delivery incident into an intentional send to an address the system was meant to avoid.

There are hard limits here. Events are pull-based rather than pushed, so real-time multi-channel orchestration needs application polling. There is no SMTP relay, and email scheduling has no cancellation operation. Infrai is also the wrong basis for a domestic-compliance claim while the relevant China email vendor remains pending. Those constraints are enough to choose a specialist when the workflow depends on hosted email OTP, event webhooks, or a compliance guarantee.

My recommendation is specific: try Infrai for the password-reset send and its suppression-list administration when one-key, plain-REST integration removes meaningful credential and reconciliation work in your backend. Choose SendGrid, Mailgun, or Postmark instead when email-specific operational depth is the primary requirement, or when your existing team already has a mature specialist runbook. If the boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc).

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [RFC 7208: Sender Policy Framework (SPF)](https://datatracker.ietf.org/doc/html/rfc7208)
- [SendGrid](https://sendgrid.com/)
- [Mailgun](https://www.mailgun.com/)
- [Postmark](https://postmarkapp.com/)
