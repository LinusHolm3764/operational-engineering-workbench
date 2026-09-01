# Gaming Webhook Retries: Queue Backoff vs Cron Sweeps for SaaS Jobs

Short answer: use a queue to hold each failed webhook and its next attempt; use cron as a periodic safety check, a redrive trigger, or an audit job. For a gaming SaaS, this is usually the simplest way to keep retry latency predictable without turning a database poller into a second queue. The decision should be made against the delivery deadline and the operational cost of duplicate effects, not against the number of configuration screens.

The failure mode is familiar. A match result is committed, the outbound webhook times out, and the receiving service may have accepted it anyway. A retry that is too fast adds load. A retry that is too slow misses the consumer's useful window. A retry that is neither bounded nor idempotent can send the same reward, score update, or entitlement event twice.

That is the runbook problem: every delivery needs a visible identity, an attempt policy, and a place to wait when the policy is exhausted. The clock can start work. It should not be the thing that owns the state of every delivery.

## Should a SaaS retry failed jobs with a queue or cron?

Choose a queue when the unit of work is one webhook delivery and the system needs to retry it independently of other deliveries. The worker receives a message, performs the side effect, and acknowledges only after the result is durably known. If the attempt is retryable, the message returns according to the backoff policy. If the attempt is permanently invalid or has reached its business deadline, it moves to a dead-letter queue (DLQ) for inspection.

That model matches the delivery contract described in the RabbitMQ consumer acknowledgement documentation: acknowledgement is explicit, and a consumer can reject or negatively acknowledge work. The important engineering consequence is at-least-once handling. A second delivery is a normal possibility, so the handler needs a stable event ID and an idempotency boundary. A unique database record, an upsert, or an idempotency key accepted by the destination can make the repeated attempt harmless.

Cron has a narrower job. It can enqueue due records, inspect DLQ volume, or initiate a controlled redrive after an incident. A cron task that scans a table and also implements leases, attempt counters, next-attempt timestamps, duplicate protection, and dead-letter states is no longer a small scheduler. It is a queue implemented by the application team, with the testing and recovery burden that follows.

This is the operational boundary to put in the runbook: cron starts or audits work; the queue represents retry state. Keep the authoritative webhook record in a database either way. The message should carry a compact reference when the payload contains mutable or sensitive game data; the worker can load the current delivery record and verify that the event is still eligible before sending it.

I've been paged by the two symptoms this design is meant to separate: a missed job and a duplicate delivery. They look similar in a log search, but the fixes are different. A missed job needs durable ownership and an observable due time; a duplicate needs an idempotency check at the side effect. Treating both as “run the cron again” is how a recovery action becomes a second incident.

## How do latency, cost, and duplicate delivery change the retry design?

Latency and cost pull in opposite directions. Short backoff gives a match-result consumer a faster update, but it can amplify an outage or rate limit. Long backoff reduces pressure and may be cheaper to operate, but the webhook can become stale before the recipient recovers. Set the maximum retry window from the business deadline: a live score update and a player-inbox notification do not need the same policy.

Use an explicit schedule rather than an unbounded loop. For example, the policy can increase delay after each transient timeout, honor a destination-supplied `Retry-After` value when present, and stop after a fixed deadline. The exact numbers belong in the service's runbook because they depend on the receiving system's limits and the game's freshness requirement. I'm not sure a universal “simplest” or “cheapest” setting exists; the useful answer is the one that makes the deadline and failure budget explicit.

Start with the receiver's timeline. Suppose a match service can tolerate a result arriving for ten minutes after finalization. A timeout at minute one can be retried quickly, while a response at minute nine should be treated as a business decision, not as permission to keep sending until the queue's retention limit. That distinction also changes alerting: an old delivery may be technically queued and still operationally lost. Record the due time and expiry time beside the event ID, then make the worker check both before it calls the destination. If the event has expired, dead-letter it with a clear reason; if it is still due, schedule the next attempt without blocking a worker. This small check prevents a recovered dependency from receiving a burst of stale game state, and it gives the on-call engineer a defensible answer to “why was this webhook not retried?” during a postmortem.

The cheaper-looking design can be expensive during recovery. A cron sweep that retries every due row at once creates a thundering herd, while a queue can distribute attempts across workers and preserve per-message visibility. A queue also does not remove the cost of idempotency, monitoring, storage, or operator time. The trade-off is worth writing down before launch.

| Decision point | Queue-owned retry | Cron-owned sweep |
| --- | --- | --- |
| Per-delivery timing | Natural fit for independent backoff | Requires application state and careful polling |
| Duplicate safety | Still requires an idempotent handler | Still requires an idempotent handler, plus claim logic |
| Recovery control | DLQ can isolate exhausted work | Table status must provide the same isolation |
| Best role | Delivery execution and bounded redelivery | Periodic audit, enqueue, and deliberate redrive |

The catch is that a queue is not a replay archive. An acknowledgement removes work from the delivery path, and retention and payload limits vary by implementation. A team that needs long historical replay, several independent consumers, or workflow joins should select a system designed for that job rather than stretching a retry queue until its semantics become unclear.

Do not sleep in the worker.

## A safe Go worker for bounded outbound retries

The worker below keeps the important boundary visible: the event ID is supplied to the destination, the handler reports a classification instead of sleeping forever, and the queue adapter decides how a retry is delayed or dead-lettered. The adapter is deliberately generic; it prevents the example from becoming a tutorial for one broker.

```go
package webhook

import (
	"context"
	"errors"
	"net/http"
	"time"
)

type Delivery struct {
	ID      string
	URL     string
	Payload []byte
	Attempt int
}

type Result int

const (
	Ack Result = iota
	Retry
	DeadLetter
)

type Queue interface {
	Ack(context.Context, Delivery) error
	RetryAfter(context.Context, Delivery, time.Duration) error
	DeadLetter(context.Context, Delivery, string) error
}

func Handle(ctx context.Context, q Queue, client *http.Client, d Delivery) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, nil)
	if err != nil {
		return q.DeadLetter(ctx, d, "invalid destination")
	}
	req.Header.Set("Idempotency-Key", d.ID)

	resp, err := client.Do(req)
	if err != nil {
		return q.RetryAfter(ctx, d, backoff(d.Attempt))
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return q.Ack(ctx, d)
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return q.RetryAfter(ctx, d, backoff(d.Attempt))
	default:
		return q.DeadLetter(ctx, d, "non-retryable response")
	}
}

func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return time.Duration(attempt) * time.Minute
}

var _ = errors.New
```

The example is a shape, not a complete policy. A production handler should send the payload, cap the attempt count, record the response classification, and use the destination's idempotency contract if one exists. It should also avoid treating every timeout as proof that nothing happened. The destination may have committed the request before the client lost the response; the event ID is what lets the second attempt be recognized.

Measure the gap.

Keep retries bounded by a business deadline. A permanent 4xx response should not keep circling because a scheduler cannot distinguish it from a timeout. Conversely, a transient 429 or 5xx response should not be dead-lettered immediately if the recipient has supplied a reasonable retry signal. Store the reason and attempt number with the delivery record so an operator can explain why a message was redriven.

## What must be verified before a DLQ redrive?

Test the failure path with a representative game event before production traffic depends on it. Force a timeout, then confirm that the same event ID is delivered again and that the receiving side records one effect. Send an invalid destination or non-retryable response, then confirm that the item reaches the DLQ with its reason intact. Finally, repair the simulated dependency and redrive a single item before touching the backlog.

Watch more than queue depth. Alert on the age of the oldest due delivery, retry volume, DLQ arrivals, the rate of duplicate suppression, and the number of items awaiting redrive. Those signals answer the incident question that matters: is the system making forward progress, or is it repeatedly doing work that cannot succeed?

Short and visible wins here.

For verification, capture the event ID, attempt number, delivery timestamps, response class, and final disposition. Check the destination for duplicate effects, not just for HTTP responses. If one event produced two rewards in the test, stop the rollout and fix the idempotency boundary; raising the retry limit would increase the damage.

Rollback means stopping the redrive trigger, preserving the DLQ items, and keeping new outbound work from entering the same failing path if necessary. Resume with a small sample and recheck the destination side effects. Do not delete the DLQ merely to make the dashboard green. That removes the evidence needed for the next decision.

For a gaming SaaS, the practical rule is stable: let the queue manage individual webhook retry state, let cron perform periodic coordination, and make duplicate delivery safe before optimizing latency or cost. The best design is the one the on-call engineer can inspect at 03:00 without reconstructing state from scattered logs.

## References

- https://www.rabbitmq.com/docs/confirms
- https://www.rabbitmq.com/docs/priority
