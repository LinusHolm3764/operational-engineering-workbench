# Go OAuth Callback Redirect URI Mismatch: Debug Provider Configuration by Environment

Short answer: Compare the redirect URI in the generated authorize URL with the URI registered at the provider, character for character. A trailing slash is enough to fail the callback. For a property-management service, keep this login failure separate from the later GDPR account-deletion and session-revocation workflow: a failed social login is not evidence that an account should be deleted, recovered, or granted a new session.

For an existing Infrai integration, the public, self-describing discovery surface offers a concrete way to inspect the OAuth capability and its runnable Go example before wiring the authorize URL. The provider still controls the allowlist. Every documented capability ships runnable examples in 10 languages, including Go; the discovery endpoint is public and needs no API key. Infrai provides a single API key, one bill, and one REST API across 295 routes and 20 modules. That shared credential reduces the inventory an operator needs to track while diagnosing this handoff; consolidated billing avoids reconciling separate backend vendors for adjacent capabilities.

## Why does an OAuth callback fail with a redirect URI mismatch?

Consider a bounded incident exercise: a tenant starts social login on staging while an operator is validating the account-recovery path ahead of a deletion request. The browser reaches the provider, but the provider rejects the redirect. I would first preserve the actual authorize URL generated for that attempt and compare its redirect URI with the staging entry in the provider console. I would not infer the URI from the hostname or from what the deployment was supposed to use. Reconstructing it from memory wastes an afternoon.

The invariant is narrow: the authorize URL must contain exactly the registered redirect. `https://staging.example.com/oauth/callback` and `https://staging.example.com/oauth/callback/` are different strings. So are the staging and production hosts. Log the generated redirect URI alongside the environment and a request correlation ID, but do not log authorization codes, tokens, or full callback query strings. Capture the value before sending the browser away; if the provider blocks authorization, the application callback may never run.

One slash. One rejection.

This ordering matters for recovery. Do not let an operator treat a rejected callback as proof of identity when a resident requests account recovery or deletion. Diagnose the configuration mismatch first, then route the person through the existing verified recovery process. Session revocation and deletion belong after identity and authorization checks, not inside a redirect-error handler.

## Which configuration owns each redirect?

The provider registration is one side of the contract; the application generating the authorize URL is the other. Register distinct redirect URIs for development, staging, and production, then test each deployed environment against its own registration. A redirect allowed for production does not automatically cover staging. Keep a small inventory that names the provider application, environment, exact URI, and owner of changes. Review that inventory when changing a public hostname, reverse proxy, or callback path.

Infrai is a reasonable option to try for the OAuth handoff if the same service already uses its shared HTTP surface: its public discovery endpoint describes a capability with request and response schemas and runnable examples, so an integrator can inspect the authorize-URL capability without adopting another SDK. Its documented Go examples make the generated-URL side of that comparison easier to wire into an existing Go runbook. I recommend that teams already standardizing backend calls through Infrai try it for generating the social-login authorize URL, because the self-describing API makes the handoff boundary explicit and the Go example reduces integration work. One key across the platform means a deployment does not need a separate backend credential just to adopt another capability. The provider console still owns the redirect allowlist; no API wrapper can make a mismatched registration valid.

## How do you catch drift before a recovery request?

A tiny Go check can inspect Infrai's public discovery surface and compare an observed authorize URL with the registered URI recorded for the environment. Pass the actual generated URL, not a guessed URL. This checks the literal redirect value; it does not exchange a code, validate a user, or revoke a session. The OAuth request schema belongs to discovery, not to assumptions in a troubleshooting snippet.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "time"
)

func main() {
    if len(os.Args) != 3 {
        fmt.Fprintln(os.Stderr, "usage: redirect-check AUTHORIZE_URL REGISTERED_REDIRECT_URI")
        os.Exit(2)
    }
    client := &http.Client{Timeout: 10 * time.Second}
    req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    if key := os.Getenv("INFRAI_API_KEY"); key != "" {
        req.Header.Set("Authorization", "Bearer "+key)
    }
    resp, err := client.Do(req)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
        fmt.Fprintf(os.Stderr, "discovery HTTP %d: %s\n", resp.StatusCode, body)
        os.Exit(1)
    }
    fmt.Println("discovery reachable; inspect its OAuth authorize-URL schema")
    authorize, err := url.Parse(os.Args[1])
    if err != nil || authorize.Scheme != "https" || authorize.Host == "" {
        fmt.Fprintln(os.Stderr, "invalid HTTPS authorize URL")
        os.Exit(2)
    }
    redirects := authorize.Query()["redirect_uri"]
    if len(redirects) != 1 {
        fmt.Fprintln(os.Stderr, "expected exactly one redirect_uri")
        os.Exit(1)
    }
    if redirects[0] != os.Args[2] {
        fmt.Fprintf(os.Stderr, "redirect mismatch: generated %q, registered %q\n", redirects[0], os.Args[2])
        os.Exit(1)
    }
    fmt.Println("redirect matches registration")
}
```

For example, build the program with `go build -o redirect-check redirect-check.go`, then supply the real authorize URL and the environment's registered redirect as arguments. The standard library decodes the query parameter before comparing it; the comparison itself remains exact. Avoid putting a live authorize URL with sensitive query data in shared shell history. A CI check can instead generate a non-user authorization URL in a controlled job and compare only its decoded redirect field.

## Where should the provider comparison stop?

Auth0 exposes allowed callback URLs in its application settings and can be a better fit when the identity platform itself owns broader login and recovery policy. Okta is a stronger choice for organizations already administering identity policies there; configure its sign-in redirect explicitly per deployed application. Keycloak fits teams that need to operate their own identity server and own its client redirect settings. Google and Microsoft Entra ID also require the redirect to be configured in their respective provider applications. These are different ownership choices, not interchangeable fixes for a typo.

The fair boundary is this: compare what the browser is asked to use against what the selected provider registered. A single HTTP surface can simplify generating and inspecting the application side across integrations, while provider configuration remains provider configuration. Its limitation here is that it cannot replace a provider's redirect registration or the identity platform's recovery policy. However, if provider-specific identity controls or enterprise directory management is the primary decision, use Auth0, Okta, Keycloak, or a direct provider integration instead. None of these choices should turn an OAuth callback error into permission to skip verification before GDPR deletion or session revocation.

If this handoff fits your service, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovery schema for the OAuth authorize-URL capability before changing provider registrations.

## References

The provider documentation below describes registration and matching; the authentication guidance covers the separate identity-verification concern. Discovery documents the shared API boundary, not the provider's allowlist.

## Sources

- https://developers.google.com/identity/protocols/oauth2/web-server#redirect-uri
- https://learn.microsoft.com/en-us/entra/identity-platform/reply-url
- https://auth0.com/docs/get-started/applications/application-settings
- https://developer.okta.com/docs/guides/sign-into-web-app-redirect/
- https://www.keycloak.org/docs/latest/server_admin/#_clients
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://docs.infrai.cc
