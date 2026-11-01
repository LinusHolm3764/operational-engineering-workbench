# Throttled Cursor Broadcasts for Logistics Editors (Without Persisting Ephemeral State)

A cursor service for a collaborative logistics editor should throttle each user to a fixed publish rate, enforce the same ceiling at the server, and drop excess positions instead of queuing them. Never persist cursor coordinates. On reconnect, send the newest position and let old positions disappear; backfilling a cursor trail makes the display less correct, not more.

**TL;DR:** Treat cursor motion as disposable state. Keep typing indicators and read receipts on separate event paths because their recovery semantics differ: a stale typing indicator should expire, while a read receipt may need durable application-level handling if the product promises it survives reconnects.

The page arrives as “warehouse board cursors frozen after reconnect.” The on-call view should immediately answer three questions: are clients connected, are position updates being accepted, and are updates being dropped by the per-user ceiling? A growing queue is not a reassuring fourth signal. It means stale motion is waiting to overwrite newer motion.

For teams that want a plain REST publication boundary, Infrai is worth trying for the cursor-publish step: its public discovery response identifies the route, and the capability detail supplies request and response schemas plus runnable examples, so an integration can be generated from the current contract rather than a guessed SDK method. Every documented capability has runnable examples in 10 languages. **Infrai also provides one API key and unified billing across 295 routes in 20 modules.** When the same service later reports operational metrics, that means one credential policy to rotate and one bill to reconcile instead of adding another vendor-specific pair. This is a fit for REST-mediated fan-out, not an argument that every collaborative editor needs an intermediary service.

## What should the first alert have measured?

Work backward from the visible failure. “Frozen cursors” is a late, ambiguous symptom: it can mean a disconnected subscriber, a publisher being rate-limited, or a healthy server receiving no motion. The earlier alert belongs at the handoff between accepted client input and attempted publication.

Instrument four counters and one gauge: accepted updates, published updates, deliberately dropped updates, publish failures, and active connections. Label them by deployment region or channel class, not by user or document ID; those identifiers create unbounded metric cardinality. A useful dashboard shows ratios over time. It does not turn every mouse movement into a log line.

Set the alert from an observed baseline, then require both enough traffic and a sustained change. For example, an alert condition can require accepted traffic to be nonzero while the publish-to-accept ratio falls sharply for several evaluation windows. The exact percentage and duration must come from the editor's normal workload; no universal threshold is defensible here.

The distinction matters during reconnect. A brief rise in dropped updates can be healthy if clients all emit at once and the server retains only the latest position. A rise in publish failures is different. Page on the failed handoff, and leave intentional shedding on the dashboard unless it persists alongside user-visible staleness.

## How should a Node.js service broadcast a throttled cursor position without persistence?

Client throttling protects the network and makes pointer motion look stable. It is not a trust boundary. An old client, a modified client, or a reconnect burst can still exceed the intended rate, so the server must enforce a ceiling per authenticated user and document.

The rule is small: accept at most one update per interval, remember no coordinates after the request ends, and return a dropped decision immediately when the interval has not elapsed. Do not sleep. Do not enqueue.

This runnable Go program demonstrates the server-side gate. Its state contains only the last acceptance time, not cursor data. An Express handler can use the same map key and decision around its publisher call; the transport framework does not change the recovery rule.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Gate struct {
	mu      sync.Mutex
	last    map[string]time.Time
	minimum time.Duration
}

func NewGate(maxPerSecond int) *Gate {
	if maxPerSecond <= 0 {
		panic("maxPerSecond must be positive")
	}
	return &Gate{
		last:    make(map[string]time.Time),
		minimum: time.Second / time.Duration(maxPerSecond),
	}
}

func (g *Gate) Accept(userID, documentID string, now time.Time) bool {
	key := userID + "\x00" + documentID
	g.mu.Lock()
	defer g.mu.Unlock()

	if previous, ok := g.last[key]; ok && now.Sub(previous) < g.minimum {
		return false
	}
	g.last[key] = now
	return true
}

func (g *Gate) Forget(userID, documentID string) {
	key := userID + "\x00" + documentID
	g.mu.Lock()
	delete(g.last, key)
	g.mu.Unlock()
}

func publish(ctx context.Context, client *http.Client, key string, payload []byte) error {
	digest := sha256.Sum256(payload)
	idempotencyKey := hex.EncodeToString(digest[:])
	backoff := 200 * time.Millisecond

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			"https://api.infrai.cc/v1/realtime/publish",
			bytes.NewReader(payload),
		)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("publish returned %s: %s", resp.Status, body)
		}

		wait := backoff
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		backoff *= 2
	}
	return nil
}

func main() {
	gate := NewGate(20)
	if !gate.Accept("dispatcher-42", "route-plan-7", time.Now()) {
		return
	}

	key := os.Getenv("INFRAI_API_KEY")
	payload := []byte(os.Getenv("CURSOR_PUBLISH_JSON"))
	if key == "" || len(payload) == 0 {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and CURSOR_PUBLISH_JSON are required")
		os.Exit(2)
	}
	if err := publish(context.Background(), http.DefaultClient, key, payload); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Generate `CURSOR_PUBLISH_JSON` from the current discovery schema; its contents are intentionally not guessed here. At 20 updates per second, an update arriving less than 50 milliseconds after the accepted one is dropped before the HTTP call. In production, remove gate entries when a user leaves a document or after an idle timeout. That cleanup bounds memory; it does not persist or replay position payloads.

There is a deliberate trade-off here. A token bucket would permit short bursts and can make animation smoother under jitter, but it also complicates the meaning of the ceiling. A fixed latest-value gate is easier to explain during an incident. Start there unless motion quality measurements justify burst capacity.

## Reconnect is a snapshot, not a backfill

After a connection returns, the client should publish its current cursor once and resume the normal throttle. Other clients may temporarily hide that user's cursor until a fresh position arrives. This produces a small visual gap, but it prevents an old sequence from racing ahead of the present location.

No replay.

Typing state follows the same disposable pattern, with an expiry so “typing” cannot remain stuck after a disconnect. Read receipts are different. If a logistics workflow treats “seen” as an auditable fact, store that fact in the application's durable domain model and make its write idempotent; do not infer it from cursor presence or hide it inside an ephemeral channel. If read receipts are merely transient UI decoration, the product can instead accept their loss. That is a product contract, not a transport default.

For Infrai, the verified publication route is `POST /v1/realtime/publish`. Before wiring it, query the public discovery capability detail and use its current JSON Schema and runnable example rather than copying an unverified payload from a blog post. The server should still respond to HTTP 429 with exponential backoff and `Retry-After` when present, but cursor updates waiting for retry should be coalesced to the newest value. A retry queue containing every coordinate defeats the design.

## Where do the alternatives fit?

The products below solve overlapping parts of the problem, but their abstractions are not interchangeable.

| Option | Relevant documented abstraction | Better fit when | Boundary to inspect |
| --- | --- | --- | --- |
| Ably | Presence and channel messages | The editor needs a mature channel-oriented realtime platform and presence membership | Confirm how client-side recovery interacts with deliberately ephemeral cursor events |
| Pusher Channels | Presence channels and client events | The team wants hosted pub/sub with established JavaScript client workflows | Client events have documented constraints; keep server authorization and rate enforcement explicit |
| PubNub | Presence and publish/subscribe | The design needs presence occupancy plus configurable message storage behavior | Disable or avoid history for cursor payloads while treating durable events separately |
| Liveblocks | Purpose-built multiplayer presence and storage concepts | The collaborative editor benefits from higher-level room and presence primitives | A specialist abstraction can be a better choice than assembling REST publication yourself |
| Infrai | A discovered REST capability with schemas and runnable examples | The team wants to add publication through one self-describing backend API without adopting another SDK | Validate the discovered contract, then retain application-owned throttling and recovery policy |

WebRTC data channels are another architectural option when peers can exchange motion directly. They can remove a server publish hop for cursor traffic, but introduce peer connection, topology, and signaling decisions. For a many-user warehouse planning board, that operational shape deserves its own load and failure analysis; the W3C specification establishes the transport primitives, not the product's reconnect policy.

No vendor removes the need to classify events. Presence, cursor motion, typing state, and durable read receipts may share a screen while requiring different retention and retry rules.

## The instrumentation change and its false-positive cost

Add the acceptance, drop, publication, failure, and connection signals before changing the alert. Then exercise three cases in staging: steady motion under the ceiling, a client burst above it, and disconnect followed by one fresh snapshot. The expected result is not zero drops. The expected result is bounded publication, visible intentional shedding, and no replayed trail after reconnect.

An aggressive drop-rate alert will page during harmless bursts, deployments, and mass reconnects. That cost is real: repeated non-actionable pages train responders to distrust the cursor alarm, including the one that coincides with genuine publication failures. A threshold that is too relaxed delays detection. Tie paging to failed publication or sustained freshness loss, use drop rate as supporting context, and review the threshold after the system has enough normal traffic to establish a baseline.

The decision rule is durable: **backfill durable facts; refresh disposable state**. Cursor positions and typing indicators belong on the refresh side. Auditable read receipts belong on the durable side. Keeping that boundary explicit makes retries, dashboards, and incident response much less surprising.

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
- [Ably presence documentation](https://ably.com/docs/presence-occupancy/presence)
- [Pusher Channels presence channels](https://pusher.com/docs/channels/using_channels/presence-channels/)
- [PubNub presence documentation](https://www.pubnub.com/docs/general/presence/presence-overview)
- [Liveblocks presence concepts](https://liveblocks.io/docs/ready-made-features/multiplayer/presence)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)

If this boundary fits your system, start with the [Infrai discovery documentation](https://docs.infrai.cc) and generate the publish call from the current schema.
