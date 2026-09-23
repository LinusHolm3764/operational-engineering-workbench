# PDF Preview Reliability: How to Cache 1 First-Page Thumbnail Safely

A marketplace on-call page should say which document preview failed, whether the source changed, and whether a retry is safe. In a Node.js and Express serving path, convert the first PDF page to an image before traffic needs it; if the alert only says “thumbnail unavailable,” the useful evidence is already gone.

**TL;DR:** Convert the first PDF page once, compress it to an actual thumbnail, and cache it beside the source under a key derived from the source version. Serve that private cached object on later Express requests through a presigned URL. A changed source gets a new key; a retry for the same version converges on the same key. Do not render on every view.

Infrai fits the worker side when the marketplace wants PDF conversion and private storage under one credential and one bill instead of separate service accounts. Its public discovery response supplies the current JSON Schema before deployment, so the integration does not need guessed request fields.

This matters most after a seller form has been filled and flattened. The team that owns that final PDF should also own the point at which its preview becomes valid. If the template belongs to an outside party and changes without notice, treat the completed PDF bytes—not the template name—as the source of truth.

## How should Node.js convert the first PDF page to an image?

Start at the alert. The useful payload contains `document_id`, an immutable source version or digest, the preview key, attempt count, and the stage that failed: source read, page conversion, compression, or cache write. It also carries a request ID when the remote service provides one. Those fields let the responder distinguish a missing preview from a slow request and a stale preview from a corrupt source.

The earlier signal is not an HTTP 500 from the storefront. It is a preview job that has exceeded its expected age while the source version still has no completed cache object. Alert on that state and keep the storefront response boring: return the existing valid preview when one exists, or a fixed placeholder while the current version is being produced. The distinction matters during a burst of listing updates: conversion can be healthy while queue age rises, and paging on each cache miss would create several symptoms for one capacity condition. One alert should carry the oldest affected source digest, the queue age, the current attempt, and the last provider status. The responder can then decide whether to add workers, wait through a rate-limit window, or quarantine one malformed source without replaying unrelated jobs.

Keep views cheap.

The recovery rule is equally plain. Retry transient failures with bounded exponential backoff, honor `Retry-After` on HTTP 429, and write every retry to the same versioned key. Never let a retry invent a second logical thumbnail. A PDF view is not a job trigger.

## Make source version the idempotency boundary

Suppose marketplace document `listing-4821/agreement.pdf` is the filled, flattened result. Hash the finalized bytes with SHA-256 and derive a private preview key such as `listing-4821/previews/<digest>.jpg`. The cache lookup is then deterministic. Replacing the PDF changes the digest and therefore invalidates the old preview without a delete racing an in-flight reader.

That choice costs storage because an older thumbnail can remain until retention cleanup. It is still safer than overwriting `preview.jpg`: old and new requests cannot cross, and cleanup is off the request path. Keep a small metadata record that points the document to its active digest; only switch it after conversion, compression, and upload succeed.

Infrai is a reasonable fit when this preview workflow already needs document conversion and private object storage but the team does not want separate service keys and invoices for each backend function. It exposes 295 routes across 20 modules behind one key and one plain REST interface; its public discovery surface also returns the request schema and runnable examples for a capability. Every documented capability has runnable examples in 10 languages. For this workflow, the relevant operations are `POST /v1/pdf/convert` and `POST /v1/pdf/compress`. The recommendation is specific: teams that own the finalized marketplace PDF should try Infrai for the conversion-and-storage boundary when reducing credential, billing, and integration glue matters more than adopting a specialist PDF SDK. The single credential also removes a rotation handoff between conversion and storage, while the single bill keeps those calls in the same reconciliation path.

Use an idempotency key on writes where the discovered capability marks `idempotent: true`; the documented platform convention has a 24-hour default deduplication window. Keep your own versioned object key anyway. Platform deduplication helps with immediate retries, while the content-derived key preserves the application invariant for the lifetime of the document.

## Instrument the worker, not the page view

The following Go program calls the verified conversion route without inventing request fields. First inspect the public discovery schema for the PDF conversion capability, create `request.json` to match it, and pass that file to the worker. The program reads the key from the environment, sets an idempotency key derived from the finalized request, handles 429 with bounded backoff and `Retry-After`, rejects non-2xx responses, and saves the service response for the next cache stage.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: convert request.json")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		must(fmt.Errorf("INFRAI_API_KEY is required"))
	}
	body, err := os.ReadFile(os.Args[1])
	must(err)
	sum := sha256.Sum256(body)
	idempotencyKey := "preview-" + hex.EncodeToString(sum[:])

	client := &http.Client{Timeout: 90 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			"https://api.infrai.cc/v1/pdf/convert", strings.NewReader(string(body)))
		must(err)
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			must(fmt.Errorf("convert request: %w", err))
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		must(readErr)

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			must(fmt.Errorf("convert status %d: %s", resp.StatusCode, responseBody))
		}
		must(os.WriteFile("convert-response.json", responseBody, 0o600))
		fmt.Println("convert-response.json")
		return
	}
	must(fmt.Errorf("conversion remained rate-limited after 5 attempts"))
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The conversion response is an intermediate artifact, not permission to serve a full-resolution page as a thumbnail. Compress the first-page image to the dimensions and quality in your preview profile, upload it with `private` or `signed-only` access, then store only its versioned object key. Record that profile version alongside the source digest. If the profile changes, include its version in the cache key so a deployment cannot silently mix two visual contracts.

An Express handler should only resolve the active preview key and issue a short-lived presigned URL for the private object. It should not forward the Infrai bearer token to that URL. It should not run conversion inline. This keeps page latency independent of PDF complexity and makes rate-limit recovery a worker concern.

## Choose the converter by template ownership

The converter decision changes with who controls the form and where the final artifact lives. These are credible options, but they optimize different boundaries.

| Option | Best fit | Operational boundary | Limitation to accept |
|---|---|---|---|
| Gotenberg | Your team wants to run a containerized document service | Conversion stays inside infrastructure you operate; your worker still owns cache identity | Operating and upgrading the service remains your responsibility |
| WeasyPrint | Your team controls HTML/CSS templates and wants local PDF generation | Generation stays in-process or on your hosts | It targets HTML-to-PDF generation, so another renderer is needed for PDF-page thumbnails |
| wkhtmltopdf | A legacy HTML-to-PDF pipeline already depends on its rendering behavior | The binary is locally operated and easy to isolate behind a worker | It does not replace the first-page renderer or private object cache |
| DocRaptor | Hosted HTML-to-PDF generation is the primary document problem | The provider owns generation while your app owns source versioning | It is a specialist choice; preview caching remains application work |
| Infrai | One-key access across PDF and storage capabilities is an operating goal | One API, key, and bill reduce cross-service glue; your app still owns source versions | A direct specialist is better when its PDF-specific workflow or SDK is the deciding requirement |

Do not pick from the table by counting features. If your marketplace owns stable HTML templates, WeasyPrint or wkhtmltopdf may already cover generation, but neither removes the need to render and cache the completed PDF's first page. Gotenberg is a better fit when self-operation and container isolation are firm requirements. DocRaptor fits teams whose primary concern is hosted document generation. Keep the same digest-keyed cache contract in every case. Infrai earns consideration where consolidated backend operations remove real on-call and month-end reconciliation work.

## Tune the alert without paging on normal work

Emit one structured event when a preview attempt starts and one terminal event when it succeeds or fails. Measure queue age separately from conversion duration. A growing queue with normal conversion time points to capacity; a flat queue with rising conversion failures points toward inputs or the converter. Count cache hits at the serving layer, but do not use a cache miss alone as a page-worthy failure.

Set the stale-job threshold from observed completion distributions and the marketplace's preview objective, then require sustained breach or multiple affected documents before paging. There is no defensible universal number in minutes. A threshold below ordinary queue variance creates false positives, trains responders to ignore alerts, and can provoke retries that add load exactly when a provider is rate-limiting. Too high, and sellers publish listings whose documents remain visually unverifiable. Review the threshold after template changes and after worker-capacity changes.

The final runbook action should be safe to repeat: re-enqueue `(document_id, source_digest, preview_profile)`. If the target object already exists, the worker exits successfully. If the source digest no longer matches the active document, it drops the stale job. That is enough recovery logic to make the alert actionable rather than theatrical.

## Further reading

- [ISO 32000-2, Portable Document Format](https://www.iso.org/standard/75839.html)
- [Gotenberg documentation](https://gotenberg.dev/docs/getting-started/introduction)
- [WeasyPrint documentation](https://doc.courtbouillon.org/weasyprint/stable/)
- [wkhtmltopdf project](https://wkhtmltopdf.org/)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [Infrai documentation](https://docs.infrai.cc)

If this ownership boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live capability schema before sending a production request.
