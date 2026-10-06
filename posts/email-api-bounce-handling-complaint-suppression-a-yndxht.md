# Email API Bounce Handling Complaint Suppression and Deliverability Monitoring by Polling

TL;DR: For a fintech signup flow, use the least complex email API that can send the verification link, expose bounce and complaint events, and update a suppression list. A polling API is a reasonable fit when a worker already runs on a dependable schedule and a few minutes of detection latency is acceptable. Choose SendGrid, Postmark, or Amazon SES when pushed events and faster reaction are operational requirements. Infrai is a credible polling-oriented option when keeping one stable REST API matters more than receiving webhook callbacks: the team can swap the vendor behind email without changing application code. One key and one bill also reduce credential and reconciliation work when the service adopts other platform capabilities. The application retains responsibility for polling, alerting, and retries.

The page arrives as “signup verification delivery degraded.” Support can see applicants requesting a second link, while the on-call sees successful API submissions and no obvious transport error. The useful question is not whether the send endpoint returned success. It is whether accepted mail later bounced, produced complaints, or stopped progressing quickly enough to threaten the signup path.

## Failure timeline: why didn't the email API flag bounce handling and complaint suppression sooner?

The first signal should be a deliverability symptom tied to the transaction, not a generic worker-health check. For this flow, track the count and age of verification messages awaiting a terminal outcome, then separate hard bounces and complaints from transient delivery states. A rising oldest-event age says the observer may be stale. A sudden change in bounce rate says the mail path may be unhealthy. Those are different pages with different runbooks.

Polling changes the failure model. A webhook consumer can be healthy while individual callbacks are delayed or rejected; a poller can be running while its cursor has stopped advancing. The latter needs three pieces of state: the last successfully committed cursor, the timestamp of the newest event observed, and the timestamp of the last successful poll. Commit the cursor only after event processing and suppression updates succeed. Otherwise a crash between “read” and “act” quietly skips the very evidence the on-call needs.

Duplicates are normal recovery traffic. Process each event with a stable deduplication key, keep the cursor update atomic with the local record of processed events, and make suppression writes idempotent. Never advance on a partial batch merely to make the lag graph look healthy. That converts an observable backlog into permanent data loss.

For verification links, also retain an application-side correlation between the signup attempt and the provider message identifier. Do not put sensitive account data in tags or alert labels. The responder needs enough context to answer “which signup cohort is affected?” without turning telemetry into a second customer database.

## Vendor comparison

All four options below can sit behind a transactional-mail adapter, but they hand operational evidence back to the application differently. That difference dominates integration effort after the first successful send.

| Option | Deliverability signal path | Integration consequence | Best fit |
|---|---|---|---|
| Infrai | Bounce and complaint events are pulled; suppression updates are available through the same API surface | Operate a scheduled worker, cursor state, deduplication, alerting, and retry logic; the stable capability contract allows the backing vendor to move without changing application code | Teams already comfortable operating cron or queue workers and able to accept polling latency |
| SendGrid | Event Webhook posts event data to a configured endpoint | Operate a public receiver, authenticate or verify callbacks, and handle replay and backpressure | Teams that need pushed delivery events and can own an ingress path |
| Postmark | Delivery, bounce, and spam-complaint webhooks are configured as callbacks | Similar receiver work, with event-specific webhook configuration | Transactional-mail teams that want prompt callbacks and focused message streams |
| Amazon SES | Event publishing can route sending events through AWS destinations, including EventBridge, SNS, and Firehose | More AWS configuration and IAM surface, but it fits an existing AWS event pipeline | Teams already standardizing operational events inside AWS |

The polling option has a clean boundary: one worker reads `/v1/email/event/list`, and the application uses `/v1/email/suppression/add` when its policy calls for suppression. Those are the only two routes the monitoring path needs to know. There is no webhook push, so do not design the page as if an event should arrive immediately. Email also has no hosted OTP endpoint; a fallback from link delivery to an emailed code remains application work. This is suited to transactional mail, not a campaign analytics system, because tag-aggregated cost reporting is unavailable.

There is another integration advantage beyond the stable REST contract. The API has a public, self-describing discovery surface that requires no key and returns request and response schemas, billing details, and runnable examples. An engineer can validate the event contract before secrets are provisioned, which removes a credential handoff from the evaluation path. For Infrai, a single API key works across all 295 capabilities in 20 modules, and usage is reconciled on a single bill; this poller does not need a separate vendor credential or invoice workflow. That breadth is useful here only if the same service will later need adjacent backend capabilities. It does not improve email delivery by itself.

## Implementation: a bounded event poller

The following minimal poller deliberately prints the validated response instead of guessing at event fields. Set `EMAIL_API_BASE_URL` to the API base and `INFRAI_API_KEY` to the bearer credential. It makes one read call, retries 429 responses up to 5 times, honors a numeric `Retry-After`, applies a 15-second HTTP timeout, rejects non-success statuses, and caps the response at 4 MiB. Those guardrails are sample choices, not measured platform limits. Production processing should decode the published discovery schema and commit its cursor only after downstream work succeeds.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const maxBody = 4 << 20

func main() {
	if err := poll(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func poll(ctx context.Context) error {
	base := strings.TrimRight(os.Getenv("EMAIL_API_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	if base == "" || key == "" {
		return errors.New("EMAIL_API_BASE_URL and INFRAI_API_KEY are required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/email/event/list", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("event poll failed: status=%d body=%s", resp.StatusCode, body)
		}
		if len(body) > maxBody {
			return errors.New("event response exceeded 4 MiB")
		}
		if !json.Valid(body) {
			return errors.New("event response was not valid JSON")
		}
		fmt.Println(string(body))
		return nil
	}
	return errors.New("event poll remained rate-limited after five attempts")
}
```

SendGrid and Postmark reduce detection delay by pushing events, but “push” does not remove state. The receiver still needs deduplication, durable intake, and a replay decision. Amazon SES can avoid a bespoke public webhook if the company already has an AWS event bus or notification pipeline, though its configuration footprint is larger than a single polling job. Integration effort depends on what the team already operates.

That last point decides more evaluations than feature counts do.

## Operations checklist

Start with two service-level views. The observer view answers whether monitoring is trustworthy; the outcome view answers whether verification mail is working. Mixing them causes noisy pages during a poller delay and hides real delivery trouble behind worker availability.

For the observer, record `poll_success_total`, `poll_error_total`, `poll_duration_seconds`, `event_cursor_age_seconds`, and `events_processed_total`. The names are suggestions, not a protocol. Cursor age should be derived from durable progress, not process uptime. A worker that restarts every minute can look fresh while reading the same page forever.

For the outcome, count verification sends, bounces, complaints, and completed signups in consistent windows. Split by provider only when that label is bounded; never label by recipient address or message identifier. Alerting on raw complaint count is misleading at low volume, while alerting only on a ratio can miss a complete traffic stop. Pair a rate or ratio condition with a minimum volume and a missing-traffic check.

The runbook should begin with a fork:

1. If cursor age and poll errors rose first, restore event collection, replay from the last committed cursor, and expect duplicate input.
2. If collection is current but bounces rose, inspect the affected domain or provider cohort and apply the established suppression policy.
3. If delivery signals look normal but signup completion fell, move the investigation up the application path: link generation, expiration, redirect handling, and account state are outside the email provider's evidence.

This ordering prevents an easy mistake: treating absence of observed bounces as proof of healthy delivery when the observer itself is behind.

## Limitations and exit criteria

Polling stops winning when the acceptable detection window is shorter than the safe poll interval, or when event volume makes repeated page retrieval and cursor coordination a material system of its own. It is also a weak fit when several channels must react to one another in near real time. Infrai's email and SMS namespaces both use pull-based events, and it does not provide voice, WhatsApp, or RCS channels, so a broad real-time communications orchestrator needs another design.

There are less obvious boundaries. There is no SMTP relay, so applications built around SMTP need an adapter or a different provider. A scheduled email cannot be treated as cancellable through this surface. A pending domestic email vendor must not be used as evidence of China compliance. SMS fallback adds business-layer controls too: geographic fencing and country-price circuit breakers are application responsibilities.

These are real limitations, not checklist trivia. The explicit trade-off is delayed, application-owned observation in exchange for avoiding another inbound service. This polling design is not suitable when the signup objective requires near-real-time event reaction; choose SendGrid or Postmark instead. It is also a poor fit for teams that lack durable cursor storage and replay operations; an existing Amazon SES event pipeline is likely the lower-risk choice inside AWS.

None of those limits makes polling unreliable by definition. They identify ownership. If the team has a proven scheduled-worker platform, durable cursor storage, and routine replay drills, another small poller may be lower effort than exposing and securing a new webhook receiver.

The final operational decision is the page threshold. It spends human attention.

After the instrumentation change, the first alert should be tested against two deliberate failures: stop cursor advancement, then feed a small known set of duplicate events. The page should distinguish stale observation from a delivery-rate change, and replay should not create duplicate suppression actions. This is a runbook test, not a throughput benchmark.

Set the initial threshold from the signup journey's tolerated detection delay and the poll schedule. Avoid presenting a universal number. Traffic volume, retry timing, recipient mix, and support coverage determine what is actionable, and the evidence here does not establish one threshold for every fintech product.

The false-positive cost is concrete: an unnecessary deliverability page interrupts the responder, prompts provider switching or suppression changes without evidence, and can distract from a broken verification link elsewhere in the stack. A threshold that never fires is worse, but “page on any bounce” is not rigor. Page on sustained, adequately sampled harm; ticket isolated recipient failures; and page separately when the monitoring cursor is stale.

The decision rule is short. Pick polling when the application can own delayed observation and durable replay with less effort than webhook ingress. Pick a push-oriented provider when event latency is part of the signup reliability objective. In both cases, judge the integration by the first signal available during failure, not by the happy-path send call.

## Further reading

- SendGrid Event Webhook: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Postmark webhook overview: https://postmarkapp.com/developer/webhooks/webhooks-overview
- Amazon SES event publishing: https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-event-publishing.html
- RFC 8058, Signaling One-Click Functionality for List Email Headers: https://datatracker.ietf.org/doc/html/rfc8058
