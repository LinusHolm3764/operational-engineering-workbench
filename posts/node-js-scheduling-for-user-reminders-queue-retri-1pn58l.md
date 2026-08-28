# Node.js Scheduling for User Reminders: Queue Retries, Dead-Letter, and Idempotent Webhooks

The least complex option that meets a reminder system's promise is usually a durable table plus a small dispatcher. Pick a dedicated queue only after that design cannot meet your isolation or throughput needs. The real decision is not which queue has the longest feature list. It is whether your team can explain every retry, dead-lettered item, and duplicate webhook during an incident.

I have been paged for missed jobs and duplicate deliveries. In one bounded production incident, a worker lost its connection after a remote email endpoint accepted the request. The message became visible again, and the second attempt sent the same reminder. We first blamed the queue because the alert arrived as a delivery failure, then traced the timeline: the provider had a request ID, our worker had a timeout, and our database had no record tying those two facts together. Nothing was wrong with redelivery; the consumer had no durable proof of what it had already done. That is the failure I use to evaluate queue choices now.

It failed.

## What does a reminder queue need to prove before it reaches production?

Start with a written state machine, not a vendor comparison. A reminder needs a product deadline (`due_at`) and a recovery deadline (`next_attempt_at`). Those are different clocks. The first says when the user should hear from you; the second says when a failed attempt may run again. A single overloaded timestamp hides late work and makes an on-call diagnosis guesswork.

For each item, record an immutable operation key, attempt count, lease owner, lease expiry, last error class, and terminal reason. A useful path is `scheduled -> available -> leased -> sent`, with `retry_wait` and `dead_letter` as explicit outcomes. Retain enough history to answer who claimed a reminder and why it stopped. A queue's retention policy is not a substitute for this business record.

The delivery contract should be at-least-once. A worker receives a lease for a bounded interval and acknowledges only after its local transaction commits. If the process dies, the lease expires and another worker can try. Exactly-once behavior across a queue, a database, and a remote webhook or email service requires a shared transaction boundary that most systems do not have. Treat duplicate execution as normal input. That's the contract.

## How should retries, dead letters, and idempotent consumers shape the choice?

Classify failures before assigning a backoff. A `429 Too Many Requests` response is a capacity signal; honor `Retry-After` when present and add jitter so a fleet does not retry in lockstep. A malformed address or an authenticated `4xx` response is normally terminal after validation. A timeout is ambiguous: the receiver may have accepted the send even though the caller saw no response. Retrying that case requires a stable idempotency key and a bounded attempt policy.

Do not let every worker invent its own schedule. Store the next attempt and the policy version with the item. Dead-lettering should be a state transition with a reason, not disappearance into a side queue. Operators need a quarantine view, a bounded replay operation, and an audit event for each replay. A global “retry all” button turns one outage into a second one.

The queue is not suitable as the system of record when its history is hard to query or expires before a support case closes. Keep canonical reminder status in a database and use the queue for coordination. A database-backed dispatcher is a good fit for modest volume and SQL-heavy operations. A dedicated broker becomes reasonable when independent consumer groups, sustained throughput, or tenant isolation are the primary constraints. A managed task service can be the smallest operations burden when its quotas, payload limits, and retry semantics match the runbook.

The catch is important: none of these choices removes idempotency from the consumer. Stick with a simpler dispatcher when the team cannot staff another operational surface; choose a broker when contention and isolation are measurable problems, not hypothetical ones.

## The failure drill I run before selecting a queue

I test the same workload against each candidate. Schedule reminders in the future, kill workers at claim and commit boundaries, return a rate-limit response, and cut the connection after a remote send. Then I ask an on-call engineer to find overdue work without reading application logs. Can they identify the lease owner? Can they isolate one tenant? Can they replay one dead letter without changing its operation key?

This is where implementation details become operational facts. PostgreSQL's `FOR UPDATE SKIP LOCKED` is useful for queue-like tables: workers can skip rows another worker has locked. The PostgreSQL documentation also warns that this produces an inconsistent view, so it is not a general replacement for a consistent report. Claim rows quickly, commit the lease, and perform network I/O afterward. Holding a database lock while waiting for an email provider converts provider latency into database contention.

The comparison should focus on contracts rather than checkboxes:

| Decision axis | SQL dispatcher | Dedicated broker | Managed task service |
| --- | --- | --- | --- |
| Delayed reminders | Due times and user state are queryable together | Verify native delay and retention limits | Verify schedule horizon and precision |
| Redelivery | Lease and transaction behavior are visible in SQL | Defined by acknowledgments and broker policy | Defined by service lease and retry policy |
| Dead letters | You own schema, indexes, and replay controls | Often a separate queue and workflow | Usually a policy plus console or API |
| Idempotency | Unique constraints fit local writes | Consumer database still needs the key | Consumer database still needs the key |
| Operational load | Polling and lock contention are yours | Capacity, partitions, and clients are yours | Limits and dependency behavior are yours |

No row wins automatically. Measure queue depth, claim latency, lease expiry, retry age, and dead-letter rate under the failure drill. Include deployment overlap: old and new Node.js consumers may process the same schema during a rollout, so use additive fields and tolerant readers. The decision is complete only when the dashboard and runbook can explain an individual reminder.

## A Go consumer that makes duplicate work harmless

The following path records the idempotency key and an outbox row in one transaction. The queue adapter should acknowledge only when this function returns `nil`; it should redeliver an error. The remote send belongs to a separate dispatcher, where the same outbox key is sent on every attempt.

```go
package reminders

import (
	"context"
	"database/sql"
	"time"
)

type Reminder struct {
	ID           string
	OperationKey string
	UserID       string
	DueAt        time.Time
}

func PrepareDelivery(ctx context.Context, db *sql.DB, r Reminder) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO consumed_operations (operation_key, completed_at)
		VALUES ($1, NULL)
		ON CONFLICT (operation_key) DO NOTHING`, r.OperationKey)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		return tx.Commit()
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO delivery_outbox
		    (operation_key, reminder_id, user_id, due_at, state)
		VALUES ($1, $2, $3, $4, 'available')`,
		r.OperationKey, r.ID, r.UserID, r.DueAt)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE consumed_operations
		SET completed_at = CURRENT_TIMESTAMP
		WHERE operation_key = $1`, r.OperationKey)
	if err != nil {
		return err
	}
	return tx.Commit()
}
```

The duplicate branch is deliberately boring: a redelivery that finds the key does not create another outbox row. In a full implementation, distinguish an in-progress key from a completed one and expose that state to operators. The network dispatcher must also use a lease, attempt limit, response classification, and terminal update. Email acceptance is not inbox delivery, and providers differ; I'm not sure any universal end-to-end exactly-once primitive exists for email. Your mileage may vary with the receiving system.

Three words matter in the runbook: prove the effect. Test a crash before the transaction, after commit, after the remote request, and before acknowledgment. Advance a fake clock across lease expiry and backoff. Every case should leave one durable business effect, a final or retryable state, and enough metadata to explain it. When a test fails, keep the captured operation key, lease timestamps, response class, and acknowledgment offset together; that small bundle is usually more useful than a wall of interleaved worker logs, especially when an operator must decide whether a replay can safely run while a new deployment is already consuming the same partition.

## References

- MDN, “429 Too Many Requests”: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
- PostgreSQL, “SELECT — FOR UPDATE and SKIP LOCKED”: https://www.postgresql.org/docs/current/sql-select.html

## Further reading

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
- https://www.postgresql.org/docs/current/sql-select.html
