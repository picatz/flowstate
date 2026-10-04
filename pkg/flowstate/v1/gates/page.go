package gates

import (
	"bytes"
	"html/template"
	"net/http"
)

// notice is a page that says one thing: an outcome or a refusal.
type notice struct {
	Title  string
	Detail string
	Gate   *gate
}

// stylesheet is the page's only style, inline so the page needs no second
// request and no third-party load, and hashed into the policy by
// [contentSecurityPolicy]. It follows the visitor's light or dark preference
// and reads at phone width.
const stylesheet = `
:root{color-scheme:light dark;--bg:#fff;--fg:#1b1f24;--muted:#57606a;--line:#d0d7de;--ok:#1a7f37;--no:#cf222e;--card:#f6f8fa}
@media (prefers-color-scheme:dark){:root{--bg:#0d1117;--fg:#e6edf3;--muted:#9198a1;--line:#30363d;--ok:#3fb950;--no:#f85149;--card:#161b22}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.5 system-ui,sans-serif}
main{max-width:40rem;margin:0 auto;padding:1.5rem 1rem 3rem}
h1{font-size:1.4rem;margin:0 0 1rem}
.prompt{white-space:pre-wrap;overflow-wrap:anywhere;background:var(--card);border:1px solid var(--line);border-radius:.5rem;padding:1rem;margin:0 0 1rem;font-size:1.05rem}
dl{display:grid;grid-template-columns:max-content 1fr;gap:.25rem 1rem;margin:0 0 1.5rem;color:var(--muted);font-size:.9rem}
dt{font-weight:600}dd{margin:0;overflow-wrap:anywhere}
label{display:block;font-weight:600;margin-bottom:.25rem}
textarea{width:100%;min-height:5rem;padding:.5rem;border:1px solid var(--line);border-radius:.375rem;background:var(--bg);color:var(--fg);font:inherit}
.actions{display:flex;gap:.75rem;margin-top:1rem;flex-wrap:wrap}
button{flex:1 1 9rem;padding:.75rem 1rem;border-radius:.5rem;border:2px solid;font:inherit;font-weight:600;cursor:pointer;background:var(--bg)}
button.approve{border-color:var(--ok);color:var(--ok)}
button.deny{border-color:var(--no);color:var(--no)}
button:focus-visible,textarea:focus-visible{outline:3px solid currentColor;outline-offset:2px}
.detail{color:var(--muted)}
`

var pages = template.Must(template.New("pages").Parse(`
{{define "head"}}<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>{{.}}</title>
<style>` + stylesheet + `</style>
</head>
<body>
<main>
{{end}}
{{define "foot"}}</main>
</body>
</html>
{{end}}
{{define "gate"}}{{template "head" "Approval needed"}}
<h1>Approval needed</h1>
<p class="prompt">{{if .Prompt}}{{.Prompt}}{{else}}This run is waiting for your answer.{{end}}</p>
<dl>
<dt>Run</dt><dd>{{.WorkflowID}}</dd>
<dt>Step</dt><dd>{{.Step}}</dd>
<dt>Signal</dt><dd>{{.Signal}}</dd>
{{if .Starter}}<dt>Requested by</dt><dd>{{.Starter}}</dd>{{end}}
{{if not .Deadline.IsZero}}<dt>Closes</dt><dd><time datetime="{{.Deadline.Format "2006-01-02T15:04:05Z07:00"}}">{{.Deadline.Format "2006-01-02 15:04 UTC"}}</time></dd>{{end}}
</dl>
<form method="post" action="{{.Action}}">
<label for="comment">Comment (optional)</label>
<textarea id="comment" name="comment" maxlength="2000"></textarea>
<div class="actions">
<button class="approve" type="submit" name="decision" value="approve">Approve</button>
<button class="deny" type="submit" name="decision" value="deny">Deny</button>
</div>
</form>
{{template "foot"}}{{end}}
{{define "notice"}}{{template "head" .Title}}
<h1>{{.Title}}</h1>
{{if .Detail}}<p class="detail">{{.Detail}}</p>{{end}}
{{if .Gate}}<dl><dt>Run</dt><dd>{{.Gate.WorkflowID}}</dd><dt>Step</dt><dd>{{.Gate.Step}}</dd></dl>{{end}}
{{template "foot"}}{{end}}
`))

// pageName is the template a response renders.
type pageName string

const (
	gatePage   pageName = "gate"
	noticePage pageName = "notice"
)

// render writes a page whole, or a bare 500 if the template fails: a template
// is written against the types this package passes it, so a failure is a bug
// and must not leave a half-written page that looks like an answer.
func render(w http.ResponseWriter, status int, name pageName, data any) {
	var buf bytes.Buffer
	if err := pages.ExecuteTemplate(&buf, string(name), data); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = buf.WriteTo(w)
}
