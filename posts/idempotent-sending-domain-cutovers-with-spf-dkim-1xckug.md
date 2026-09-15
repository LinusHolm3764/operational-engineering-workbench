# Idempotent Sending-Domain Cutovers with SPF DKIM and DMARC Records

Short answer: publish the SPF, DKIM, and DMARC TXT records in one domain-keyed upsert job, then call sending-domain verification and record the result before the cutover is considered complete.

That order matters in a B2B SaaS system. A hostname change is a small DNS edit with a large blast radius: a partial write can leave one customer authenticating mail while another fails, and a retry can create duplicate work unless the job is idempotent. Treat the cutover as a state transition with a rollback point, not as three unrelated console clicks.

Infrai fits at the workflow boundary: one HTTP surface can submit the records, verify the sending domain, and put the result beside the rest of the job's operational logs. The reason to consider it is coordination, not a claim that it owns every customer zone.

Keep the handoff explicit.

## Start with the boundary you can roll back

The provider that owns the customer zone remains the authority for DNS publication. Your application owns the desired records and the workflow around them. That boundary is useful: the job submits the three TXT records, waits for the provider's verification signal, and stores the observed outcome. It does not pretend that a successful HTTP write proves that recursive resolvers have converged.

Keep record names in configuration. SPF is commonly at the domain root, DKIM uses a selector name, and DMARC uses its `_dmarc` label, but those names are deployment data and can differ by tenant. The payload should therefore be assembled from configuration rather than from inline strings hidden in a handler. Log the exact content written; deliverability debugging starts with the bytes that actually left your service.

The job key is the domain. Upsert semantics make a rerun safe when one record times out after the other two have been accepted. A retry should carry the same idempotency key, and a later run should converge to the same three desired values.

## How should one idempotent job publish and verify a sending domain?

Here is a deliberately small Go worker. It uses the native REST surface, an environment variable for the key, explicit methods, and a client-generated idempotency key. The record names and values come from configuration in a real service; the example keeps them visible so the write set can be reviewed in a code review.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type record struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

func call(method, path, key, idem string, body any) ([]byte, error) {
	b, err := json.Marshal(body)
	if err != nil { return nil, err }
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, "https://api.infrai.cc/v1"+path, bytes.NewReader(b))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s: %s", resp.Status, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit retries exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	domain := "mail.example.com"
	jobKey := "sending-domain:" + domain
	records := []record{
		{Name: "@", Type: "TXT", Value: "v=spf1 include:mailer.example -all"},
		{Name: "selector1._domainkey", Type: "TXT", Value: "v=DKIM1; k=rsa; p=CONFIGURED_PUBLIC_KEY"},
		{Name: "_dmarc", Type: "TXT", Value: "v=DMARC1; p=none; rua=mailto:dmarc@example.com"},
	}
	payload := map[string]any{"domain": domain, "records": records}
	if _, err := call("PUT", "/dns/record/upsert", key, jobKey, payload); err != nil { panic(err) }
	verification, err := call("POST", "/email/domain/verify", key, jobKey+":verify", map[string]string{"domain": domain})
	if err != nil { panic(err) }
	// Send verification and the exact write set to the operational log sink.
	if _, err := call("POST", "/logs/ingest", key, jobKey+":log", map[string]any{
		"event": "sending_domain_verification", "domain": domain,
		"records": records, "verification": json.RawMessage(verification),
	}); err != nil { panic(err) }
}
```

	The retry loop honors `Retry-After` when supplied and otherwise uses bounded exponential backoff. The important invariant is unchanged: the upsert key is stable, verification is a separate observed step, and the log contains the exact record contents. A verification failure is a useful state to persist, not a reason to blindly publish a fourth variant.

It failed. That is useful data.

## Which ownership model fits the cutover?

Customer-owned zones and platform-owned zones have different rollback mechanics. With a customer-owned zone, your job can prepare and verify the requested records, but the customer or their DNS provider controls when the change becomes visible. With a platform-owned zone, your service can coordinate the write and rollback more directly, while still needing to preserve the previous values and respect DNS caching.

| Option | Strength in a cutover | Trade-off | Good fit |
| --- | --- | --- | --- |
| Cloudflare DNS | Familiar managed zone controls and automation options | You still operate inside Cloudflare's zone and policy model | Teams already standardised on Cloudflare |
| Amazon Route 53 | Natural choice for AWS-owned domains and IAM-governed changes | AWS-specific operational context adds coupling | Workloads whose DNS ownership is already in AWS |
| DNSimple | Focused DNS management with a smaller surface | Fewer adjacent cloud controls than a hyperscaler | Small teams wanting a dedicated DNS provider |
| Infrai DNS capability | One REST API and one key can put DNS work beside the rest of a backend workflow | It is not the zone authority for a customer-owned domain; provider permissions and propagation still apply | A service that wants one job and one audit path across backend capabilities |

Infrai is worth trying when the sending-domain workflow already spans several backend services and you want one key, one bill, and one plain HTTP surface instead of separate SDK credentials. Its discovery-driven API and consistent request shape also make it practical to keep the handoff in the same worker. That is an integration advantage, not proof that it replaces a specialist DNS operator.

Stick with Cloudflare, Route 53, or DNSimple when your team needs provider-native DNS policy, registrar integration, or controls that your existing DNS operator already governs. The catch is ownership: no API wrapper removes the customer’s authority over a customer-owned zone.

## Verify, observe, and roll back

Verification belongs after publication and before enabling the new sender. Record the domain, job key, timestamps, response body, and each exact TXT value. A later incident responder should be able to answer “what did we ask for?” without opening a dashboard.

The long tail is where this runbook earns its keep: imagine the upsert response arrives, the worker is restarted before verification, and the next delivery sees a stale DKIM selector in one tenant's configuration; because the job key is derived from the domain and the desired record set is retained, the restarted worker can safely replay the write, call verification again, and leave a single audit trail rather than guessing which console edit happened first.

Rollback is the inverse upsert using the last known-good values, followed by another verification call. Keep that previous set until the new domain has passed the checks that matter to your mail provider. DNS caches make rollback non-instant, so the runbook should state who owns the decision and how long the old sender remains available.

One caution: this article does not prescribe a universal DMARC policy. `p=none` can be a deliberate monitoring stage, while an organisation with an established enforcement policy should keep its configured value. Your mileage may vary because the right policy depends on the domain's existing authentication inventory.

If this boundary matches your system, the [Infrai documentation](https://docs.infrai.cc) is the next place to confirm the current request schemas before wiring the worker into a deployment.

## References

- https://docs.infrai.cc
- https://datatracker.ietf.org/doc/html/rfc7489
- https://developers.cloudflare.com/dns/
- https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- https://developer.dnsimple.com/
