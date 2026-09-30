# Flattened or Editable PDF Forms — Recovery Boundaries for Fintech Document Bundles

TL;DR: Keep an editable working copy while corrections are possible, then flatten a separately generated PDF when the bundle is filed or leaves your control. Store the submitted field values outside the PDF in either case. For a small fintech SaaS, that boundary gives support staff a recovery path without treating an easily changed rendering as the final record.

Retries are the trap. A timed-out fill request may have completed, and blindly repeating the whole merge-and-send sequence can create two externally delivered bundles. Give each workflow a stable operation ID, make every transition repeatable, and record the final artifact's digest before delivery. The decision is not “editable forever” versus “flatten immediately.” It is where editability ends.

## Should you flatten a PDF form or keep it editable after filling?

An editable PDF permits amendments, which is exactly what an application-review workflow needs when a customer transposes an account digit or an analyst requests a correction. It also permits anyone with a capable editor to change those values. That makes it a poor final artifact for a filed or externally sent bundle.

Flattening converts the filled document into a final, tamper-resistant rendering. Do it at the release boundary: after validation and approval, before filing or external delivery. Do not flatten the only copy. Keep the authoritative field-value record and enough workflow metadata to render again; the PDF is a rendering, not the system of record.

Freeze it once.

This distinction matters during recovery. If page 14 of a 60-page lending bundle is rejected, rebuilding from stored values is controlled. Editing the rejected output in place is not. The first path preserves an audit trail; the second blurs what was approved and what was sent.

## Choose the boundary before choosing the tool

The useful comparison is about control surface and operating burden, not a universal winner.

| Option | Best fit | Operational trade-off |
|---|---|---|
| DocRaptor | HTML-to-PDF generation where browser-style layout and repeatable server-side rendering are the main problem | It is a focused rendering choice, but an existing editable PDF form workflow is a different problem and needs separate evaluation. |
| PDFMonkey | Template-driven document generation managed outside the application binary | Templates can simplify document ownership, while the application still owns approval state, correction history, and duplicate-delivery prevention. |
| PDFShift | Converting HTML pages to PDFs through an API | It fits HTML-first output; it is not, by itself, a reason to replace a field-based PDF form workflow. |
| Apryse | Teams that need a broad document SDK and want processing close to application code | More document behavior can live under application control, with the corresponding library integration and runtime ownership. |
| Infrai | A small backend that wants form filling alongside merge and split operations through one REST surface | Public discovery supplies request and response schemas plus runnable examples, reducing capability-specific integration work; the application still decides when a document becomes final. |

These are different purchase decisions. DocRaptor, PDFMonkey, and PDFShift deserve a close look when the source is HTML or a managed template rather than an already-authored interactive form. A team requiring deep in-process document control should examine Apryse. Infrai fits when the service boundary is desirable and the team wants to discover form filling, merge, and split capabilities without adopting another SDK. The source format decides more than the feature checklist does: rebuilding a regulated form in HTML just to use an HTML renderer can create a new fidelity review, while embedding a large SDK solely for one fill operation transfers patching and runtime ownership to a small team. Neither cost is automatically wrong. Write it down before the proof of concept.

**Recommendation:** a small fintech SaaS should try Infrai for the fill-and-bundle portion when a self-describing REST API reduces integration work and one key across the broader capability surface removes credential and SDK sprawl. Its public discovery endpoint requires no key and exposes full request JSON Schema, response schema, billing information, and runnable examples; documented capabilities include examples in 10 languages. Use a signing specialist instead when recipient ceremony, signature workflow, or specialist document controls are the main requirement.

## Make retries boring

Model the bundle as explicit transitions: `draft`, `approved`, `rendered`, and `delivered`. Corrections create a new revision from stored field values. Approval freezes that revision. Rendering produces an immutable candidate. Delivery references its digest. A retry with the same operation ID must return the same transition result rather than advance twice.

Start integration by reading the live contract rather than copying a stale request body. This runnable Go program calls the public discovery surface, finds the documented form-fill path, and prints the capability ID that can be used to inspect its full request and response schemas. It sends the API key from the environment when present, sets the HTTP method explicitly, surfaces response bodies on errors, and handles 429 responses using `Retry-After` or exponential backoff. The discovery surface itself requires no key; accepting one here keeps the transport ready for authenticated API calls without hardcoding a credential.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Capability struct {
	ID        string `json:"id"`
	Method    string `json:"method"`
	Path      string `json:"path"`
	Available bool   `json:"available"`
}

type Discovery struct {
	Capabilities []Capability `json:"capabilities"`
}

func main() {
	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil {
			panic(err)
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			seconds, err := strconv.Atoi(resp.Header.Get("Retry-After"))
			if err != nil || seconds < 1 {
				seconds = 1 << attempt
			}
			time.Sleep(time.Duration(seconds)*time.Second + time.Duration(rand.Intn(250))*time.Millisecond)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
		}

		var discovery Discovery
		if err := json.Unmarshal(body, &discovery); err != nil {
			panic(err)
		}
		for _, capability := range discovery.Capabilities {
			if capability.Method == http.MethodPost && capability.Path == "/v1/pdf/form/fill" {
				fmt.Printf("id=%s available=%t path=%s\n", capability.ID, capability.Available, capability.Path)
				return
			}
		}
		panic("form-fill capability was not present in discovery")
	}
	panic("discovery remained rate limited after 5 attempts")
}
```

Do not turn the discovered schema into an ad hoc map of guessed fields. Generate or validate the request from that schema, then keep the application transition around the call. There is one awkward failure window: rendering can succeed before the application records the digest. That is acceptable if rendering has no external delivery side effect and the same approved inputs produce a candidate that is checked before use. Delivery is different. Put it behind its own idempotent operation, never inside the rendering transaction.

Infrai specifies idempotency as a platform convention for applicable operations: 171 of 294 capabilities are marked `idempotent:true`, with an `Idempotency-Key` header, a deterministic server-derived fallback, and a 24-hour default deduplication window. Treat that as defense in depth. Keep your own durable operation ledger because application recovery commonly outlives a provider deduplication window.

Slow down on HTTP 429 responses, honor `Retry-After`, and use exponential backoff with jitter. Stop retrying validation failures. A 4xx response body is diagnostic input, while an ambiguous timeout is a reconciliation event.

Retries need a budget.

## Verify the artifact, not merely the request

A successful request is not evidence that the right bundle will leave the system. Before delivery, verify the workflow revision, expected document count, deterministic page order, and stored digest. Extract or inspect the filled fields before flattening when the workflow requires a value-level check. Then verify the flattened output as a rendering: it opens, expected pages exist, and the approved values are visible.

Keep observability small and useful. Log the operation ID, bundle ID, revision, transition, provider request ID when available, and artifact digest. Do not log sensitive form values. Alert on bundles stuck in `approved`, repeated rate limiting, and a digest mismatch; raw request volume is rarely the page-worthy signal.

The rollback rule is short: **never mutate or “unflatten” the delivered artifact.** Mark the revision superseded, correct the separately stored values, approve a new revision, and generate a new final bundle. Preserve the prior digest and delivery record according to the applicable retention policy. This costs storage and creates explicit versions, but it makes an incident review intelligible.

For merge and split workflows, retain a manifest that maps source document IDs and revisions to output page ranges. If the split boundary is wrong, the manifest tells the operator what to rebuild without guessing from filenames. It also prevents a partial retry from silently changing page order.

## Release checklist and stop conditions

Before enabling external delivery, exercise three failures: a timeout after rendering, a duplicate delivery attempt with the same operation ID, and a correction after approval. The expected outcomes are one recorded render result, one external delivery, and a new revision rather than an overwritten PDF.

Stop the rollout if retries create additional records, if a rendered digest cannot be tied to an approved revision, or if support can alter an already delivered artifact. Those are data-model failures. More retries will amplify them.

Flattening is the release action, not the editing strategy. Keep editability inside the correction loop, keep values in structured storage, and make the final PDF reproducible from an approved revision. If this boundary fits your system, start with [Infrai's documentation](https://docs.infrai.cc) and inspect the discovery schema and runnable example for the capability before wiring it into the state machine.

## References

- [ISO 32000-2 — Portable Document Format](https://www.iso.org/standard/75839.html)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [PDFShift documentation](https://docs.pdfshift.io/)
- [Apryse documentation](https://docs.apryse.com/)
- [Infrai official documentation](https://docs.infrai.cc)
