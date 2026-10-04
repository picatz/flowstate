package gates

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
)

// contentSecurityPolicy is sent with every page. It permits no script, no frame
// around the page, no base rewrite, no form that posts elsewhere, and exactly one
// inline stylesheet, named by its hash so that markup injected into a prompt
// could not add another.
var contentSecurityPolicy = func() string {
	sum := sha256.Sum256([]byte(stylesheet))

	return "default-src 'none'; style-src 'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) +
		"'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
}()

func setSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Security-Policy", contentSecurityPolicy)
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Frame-Options", "DENY")
}

// fromThisPage reports whether the browser vouches that r was made from a page
// of this origin.
//
// Fetch Metadata is the primary signal: a browser that sends Sec-Fetch-Site
// says whether the request was same-origin, and it is the browser's statement,
// which a page on another origin cannot forge. A browser too old to send it
// still sends Origin on a form POST, and that is compared with the host the
// request was addressed to. A request with neither is refused: it was not made
// by a browser following a form, and a client that is not one uses the API.
//
// Stateless on purpose. A token minted at render time would need a key every
// replica shares, or sticky routing, to survive a gate page opened on one and
// answered on another.
func fromThisPage(r *http.Request) bool {
	switch site := r.Header.Get("Sec-Fetch-Site"); site {
	case "same-origin":
		return true
	case "":
	default:
		return false
	}

	origin := r.Header.Get("Origin")
	if origin == "" || origin == "null" {
		return false
	}

	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}

	return u.Host == r.Host
}
