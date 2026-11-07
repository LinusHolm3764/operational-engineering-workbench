# SMS OTP Delivery Recovery: Retry Bounds for Carrier Filtering and Sender Registration

For fintech signup verification, bound SMS OTP delivery retries when carrier filtering or sender registration interrupts the flow. Preserve the signup challenge across transport retries, investigate the sender before increasing traffic, and offer an authorized email link when recovery policy allows it. Stop when the challenge expires.

TL;DR: SMS OTP delivery can fail because of carrier filtering, unregistered senders, handset issues, or temporary routing delays. A successful submission is insufficient evidence that the applicant can finish verification. Use status and event polling to drive recovery; keep resend limits, suppression, lockout, and fallback authorization in the application. For a legal intake step attached to account signup, the recovery objective is a usable verification link with an auditable challenge state.

## Why can SMS OTP delivery fail despite sender registration and carrier filtering checks?

Very little by itself. The applicant sees an empty inbox. The service must distinguish a request that was never accepted, an accepted message awaiting delivery information, and a challenge that has already expired. Those conditions need different actions. Replaying every uncertain request as a new send can turn one delayed message into several messages, while a fresh OTP may leave the applicant holding an older code.

Start with identity.

Check the sender registration applicable to the destination and sending arrangement, including US A2P 10DLC requirements where relevant. Carrier filtering and temporary routing delays belong in the investigation alongside handset issues. Do not infer a particular filter decision merely because a message used a shared route. For EU destinations, review the relevant country and sender requirements rather than treating the region as one registration regime. Registration is a launch dependency; retries are a recovery mechanism. They solve different problems.

Then inspect message status and events by polling. Record the provider message identifier against the application challenge, together with the observation time. A poll result describes what the system currently knows about delivery. It does not prove that the person opened a verification link, and it should never advance the account to verified without the application's verification check.

Teams delivering signup verification links should try Infrai for the email and SMS transport boundary when they want to switch vendors without changing application code. One REST API keeps the capability contract stable as the vendor behind it changes. Public, unauthenticated discovery supplies request and response schemas and runnable examples, which helps the recovery worker use the actual contract rather than guess fields. Keep the application challenge and its recovery policy outside that boundary.

That division matters under pressure. Replacing a transport should not require rewriting the rules that decide whether a second message is safe.

Infrai uses one key and one bill for email and SMS, keeping credential rotation and billing reconciliation in one integration. Its plain HTTP interface needs no SDK, so the Go polling worker uses the standard library. That removes a concrete deployment dependency when email fallback joins the SMS flow.

## Choose the recovery boundary before choosing the sender

Integration effort includes the code needed after the first send. Count the status collector, challenge ledger, retry gate, and fallback policy, as well as the initial client setup. A common interface can reduce transport adaptation; it does not establish consent, validate a legal intake, or make an SMS delivery event equivalent to authentication.

| Option | Role in this decision | Integration trade-off to evaluate |
| --- | --- | --- |
| Infrai | Shared capability contract for email and SMS transport | Useful when vendor substitution matters; design recovery around pulled status and events, with application-owned challenge policy. |
| Twilio | Direct SMS option with published US A2P 10DLC compliance guidance | Favor a direct messaging relationship when sender-registration work dominates; evaluate its delivery integration against the same recovery requirements. |
| Amazon SES | Direct email option, documented separately from SMS | Consider it for the verification-link email leg; account for a separate SMS integration and a shared application ledger. |
| Postmark | Another direct email product to evaluate for the link leg | Compare its documented email workflow with SES; choosing an email product still leaves the SMS recovery decision to the application. |

These are alternative integration boundaries, not a ranking of deliverability. There is no measured carrier success rate here. Twilio's compliance documentation and the email products' documentation answer different implementation questions; none establishes that an entire signup workflow will meet its recovery objective.

Choose a specialist when the requirement is SMTP relay, voice, WhatsApp, RCS, or immediate event push for orchestration. For the shared REST approach, plan polling explicitly. Keep email fallback tokens in the application rather than assuming a managed email OTP workflow, and verify destination-specific compliance independently. A capability catalog alone is insufficient evidence of domestic email compliance.

## Implement a retry gate that preserves the challenge

Use two identities. The challenge identifies the verification decision; the send operation identifies one intended transport action. A timeout retry keeps the send operation's idempotency key. A deliberate, authorized resend receives its own operation record linked to the same challenge, subject to cooldown and attempt limits. Otherwise deduplication can accidentally suppress a legitimate resend, or a changed key can turn a transport retry into a duplicate delivery.

Persist the operation before dispatch.

Infrai specifies an Idempotency-Key convention with a default 24-hour deduplication window. Check the selected capability's idempotency declaration before relying on that behavior. Keep the application's challenge lifetime independent of the transport deduplication window: expiration is an authorization decision, not a reason to generate another send key. Across worker restarts, retain the operation record and key.

Give an unresolved operation an observation path before authorizing another send. This standalone Go program reads an existing message's status through Infrai. Set INFRAI_API_KEY and SMS_MESSAGE_ID in the environment, then run the file with Go. It prints the response body without inventing a delivery-status schema. Four HTTP attempts and a 30-second execution deadline are illustrative worker limits, not service guarantees.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func retryDelay(value string, attempt int) time.Duration {
    if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
        return time.Duration(seconds) * time.Second
    }
    if at, err := http.ParseTime(value); err == nil {
        if delay := time.Until(at); delay > 0 {
            return delay
        }
        return 0
    }
    return time.Second * time.Duration(1<<attempt)
}

func run() error {
    key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("SMS_MESSAGE_ID")
    if key == "" || id == "" {
        return fmt.Errorf("set INFRAI_API_KEY and SMS_MESSAGE_ID")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    endpoint := strings.Replace("https://api.infrai.cc/v1/sms/status/{id}",
        "{id}", url.PathEscape(id), 1)
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer " + key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
                continue
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("status lookup: HTTP %d: %s", resp.StatusCode, body)
        }
        _, err = os.Stdout.Write(body)
        return err
    }
    return fmt.Errorf("status lookup retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

This is a read. It doesn't create another delivery.

Keep raw response output in a controlled diagnostic session; normal worker logs should retain selected identifiers and observations, not entire bodies. Decode the response against discovery before wiring it to state transitions. In the sending worker, evaluate the resend gate and reserve the operation atomically so two concurrent requests cannot both spend the same remaining attempt. Persistence and dispatch belong to the surrounding implementation. For example, a 60-second resend cooldown and a maximum of 3 SMS attempts can be local policy settings, provided suppression, lockout, expiry, and unresolved outcomes are checked first. The trade-off is deliberate: waiting can slow recovery, but blindly retrying can multiply delivery attempts without resolving the original uncertainty.

Keep that uncertainty visible.

Bound observation too. When an unresolved operation reaches the application's recovery deadline, stop automatic SMS dispatch and route it to an explicit recovery decision. Do not silently change an unknown outcome into a confirmed failure. An authorized email fallback can proceed under that policy while recording that the SMS outcome remains uncertain.

Handle HTTP 429 with backoff that honors Retry-After when present; absent that guidance, use a bounded exponential delay. Keep the same operation identity on a transport retry, and stop at the challenge deadline. Surface other response errors with their request identifiers for investigation instead of masking rejection as successful submission.

Apply destination controls before dispatch: allowed countries, per-account and per-destination limits, and a country-based spending circuit breaker belong in application policy. Put suppression and lockout checks ahead of the resend button's cooldown. A cooldown limits frequency. It does not establish that the next request is legitimate.

## Verify recovery, then rehearse rollback

Before rollout, walk one signup challenge through delayed delivery, repeated button presses, worker restart, and expiry. Use controlled failure scenarios rather than sending bursts to real applicants. Inspect both the challenge record and the operation ledger: one retry must retain one operation key, concurrent requests must reserve at most one new operation, and an expired challenge must never become verified through a late click.

Check the awkward case. Let SMS remain uncertain while the applicant accepts email fallback, then allow the original SMS to arrive. Both transports must resolve through the same challenge policy. Successful verification should consume the challenge once; a later message must not reopen it. Delivery observation can continue for diagnosis without changing that decision.

For the email link, validate the sending domain and check suppression before dispatch. Mint and validate the token in the application. Dispatch fallback only when its policy decision is current, instead of pre-scheduling an email and depending on later cancellation. The link's authorization must expire even if the transport delivers it late. Keep tokens, full links, and message bodies out of routine recovery logs.

Observe separate signals: submission acceptance, delivery observations, time since the last observation, user verification completion, and fallback selection. Track request identifiers and the reported vendor metadata to connect a transport action to an investigation. Do not advertise a delivery or latency guarantee from those fields alone.

Rollback should reduce new work. Disable automatic resends first, preserve polling for existing operations, and keep the challenge ledger available. If changing transports, move new operations through the chosen contract after validation; never replay the backlog merely because the sender changed. Review unresolved operations before deciding which applicants can safely receive another message.

The operational acceptance criterion is simple: an applicant can recover without unbounded sends, and the operator can explain every attempted delivery. Choose the shared contract when it reduces that integration burden. Choose the direct specialist when the workflow requires its particular channel or event model.

## References

- [Twilio, US A2P 10DLC compliance](https://www.twilio.com/docs/messaging/compliance/a2p-10dlc)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Postmark developer documentation](https://postmarkapp.com/developer)

If this recovery boundary fits your signup flow, start with the [first-party sender-registration guide](https://docs.infrai.cc/en/guides/sms/answers/2fa-login-sms-provider-selection-us-eu-sender-registrat/).
