# Scheduled Data Cleanup Queue Runbook for EU and US SaaS

Use one authoritative schedule, a fixed cleanup cutoff, and a durable run record before debating queue software. Short answer: for scheduled data cleanup in a small SaaS spanning EU and US data, the easiest and cheapest design is the one whose duplicate delivery, missed-window recovery, and residency ownership the team can prove in an exercise.

That rule is deliberately unglamorous. A cron expression tells a process when to attempt work; it does not prove that work happened, decide which region owns a window, or make a repeated delivery harmless. The work contract has to do those jobs.

## The incident lesson: a successful queue can still leave data behind

Consider a bounded failure drill. At 02:14, a scheduler emits a cleanup window, a worker takes it, and the process stops after the message is accepted but before its database transaction commits. Monitoring can report a healthy scheduler and an empty queue while expired records remain. Then the scheduler repeats the same window after recovery. If the second execution cannot tell whether the first one committed, the system either skips necessary cleanup or performs an unsafe repeated effect.

The invariant is simple: the message names a logical window, while the database records the durable result of that window. A stable key such as `retention:2026-08-07T02:00:00Z` gives every retry the same identity. The worker derives current eligibility from the system of record, rather than trusting a payload full of copied tenant attributes that may already be stale.

This is the useful distinction between activity and outcome. A scheduled delivery is activity. A committed record saying which cutoff ran, under which job identity, is outcome.

Start there.

The catch is that a one-time migration or a cleanup with irreversible side effects needs a different review. Do not reuse a recurring deletion runbook for an operation that needs human approval, legal-hold checks, or a tested restore path. For ordinary retention work, though, repeated execution should converge on the same state.

## How should scheduled data cleanup work across EU and US for a small SaaS?

Choose one owner for each scheduled window. The owner can dispatch region-aware work, but two independent schedulers must not create two different identities for the same time slot. Put a uniqueness constraint on the job name and scheduled timestamp; that constraint is the last line of defense when deployments overlap, a leader changes, or an operator starts a recovery run.

Cron is a time-based job scheduler, and it remains useful as the clock. Treat it as a trigger, not as an audit log or coordination protocol. A missed tick should lead to an explicit backfill decision: create every missed hourly window, or create one documented window with a fixed cutoff. Either policy can be correct. Silence cannot.

Write it down.

For EU and US deployments, residency is part of the job definition. Keep the central run identity separate from the placement decision, then have the worker obtain tenant location from authoritative state before it touches data. This avoids making a stale message attribute the source of truth. It also makes the handoff inspectable during an incident.

A practical runbook needs these questions answered before implementation:

- Who owns the clock for this task?
- What is the immutable cutoff for a given window?
- Is a late window backfilled or coalesced?
- Which durable transition proves completion?
- Which region is permitted to read and delete each tenant's records?

Small teams often start by asking which queue is easiest. These answers decide that later. Good.

## The preventative path is a transactional claim

For modest cleanup volume, a database-backed claim can keep coordination and deletion under one transaction boundary. PostgreSQL documents that `SKIP LOCKED` returns an inconsistent view and is not appropriate for general-purpose reads; it also calls out queue-like access by multiple consumers as a case where reduced lock contention can be useful. That narrow scope matters. Use it for claiming work, not for ordinary application queries.

The following Go example first claims a logical window through a unique row. A duplicate delivery exits after finding the run already recorded. The query then locks a bounded batch of eligible records, and the delete predicate repeats the cutoff check. An empty batch is a clean completion; a nonempty batch should commit progress and arrange another bounded attempt using the same logical run identity. Don't hold one transaction open across an entire large dataset.

```go
package cleanup

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Job struct {
	Name         string
	ScheduledFor time.Time
	Cutoff       time.Time
}

func Run(ctx context.Context, db *sql.DB, job Job) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO cleanup_runs (task_name, scheduled_for, started_at)
		VALUES ($1, $2, now())
		ON CONFLICT (task_name, scheduled_for) DO NOTHING`, job.Name, job.ScheduledFor)
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

	rows, err := tx.QueryContext(ctx, `
		SELECT id FROM expired_records
		WHERE expires_at < $1
		ORDER BY id
		FOR UPDATE SKIP LOCKED
		LIMIT 500`, job.Cutoff)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM expired_records WHERE id = $1 AND expires_at < $2`, id, job.Cutoff); err != nil {
			return fmt.Errorf("delete record %d: %w", id, err)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return tx.Commit()
}
```

There is a trade-off here. A database work queue is often a sensible first design only while its row locking, deletion churn, and transactions stay away from customer-request latency. When measured contention competes with the primary workload, move the claim traffic to infrastructure with a separate operational boundary. The right move is driven by evidence from the primary database, not a preference for fewer components.

## Compare ownership costs before comparing request prices

“Cheapest” has no durable meaning until the operating boundary is named. Count the existing runtime, backups, upgrades, recovery rehearsals, alert ownership, retention storage, cross-region transfer, and the engineer time needed to explain a missed window at 02:14. A small monthly bill can be expensive if it creates a new stateful service with no recovery practice. A managed service can be the lower operational burden if its delivery, region, and retention terms meet the task's contract. Your mileage may vary because those contracts and existing team skills differ.

| Implementation boundary | Usually easiest when | What to prove before using it |
|---|---|---|
| Existing application queue | The runtime, monitoring, and recovery playbook already exist | Duplicate delivery and missed-window behavior |
| Provider-operated queue | The team accepts the provider's documented delivery and regional contract | Residency, retention, access control, and replay policy |
| Existing message broker | A shared team already owns its capacity and restore procedures | Isolation from other workloads and a tested consumer recovery path |
| Database claim table | Work volume is modest and database headroom is measured | Lock impact, transaction duration, and cleanup churn |

No row is a universal winner. Adding a broker solely for a periodic cleanup is usually hard to justify when nobody owns the broker lifecycle. Conversely, keeping all work on the primary database is not suitable when cleanup batches delay foreground queries. Both choices fail when they are treated as a library decision instead of an operational commitment.

## Test the recovery contract before deployment

Start with the duplicate: invoke the same window concurrently and verify that one durable run identity wins. Then stop a worker after it has claimed records but before commit; a later attempt must still be able to process those records. Stop it after commit and verify that the repeat produces no extra effect. Finally, skip two scheduling windows and demonstrate the explicit backfill or coalescing policy.

Observe age, not just throughput. Alert on the age of the oldest eligible record and the time since the last completed logical window, with the run identity attached. Queue depth and consumer count are diagnostics, but neither proves the cleanup result.

Deployments need the same discipline. Keep producers and consumers compatible during a rolling rollout, reject an unsupported payload before acknowledgement, and exercise the job against production-shaped data including records exactly on the cutoff and tenants in both permitted regions. Add restore evidence where deletion policy requires it.

That is the decision rule again: choose the scheduler and queue boundary that can meet these tests with clear ownership. The cleanup system earns trust from its recovery behavior, not from the shortest setup guide.

## References

- [Cron](https://en.wikipedia.org/wiki/Cron)
- [PostgreSQL SELECT documentation, including FOR UPDATE SKIP LOCKED](https://www.postgresql.org/docs/current/sql-select.html)
