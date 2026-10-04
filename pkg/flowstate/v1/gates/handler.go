package gates

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"

	"github.com/picatz/flowstate/internal/textbound"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// PathPrefix is where [Handler] is mounted. A deployment that serves the page
// routes this prefix to it and nothing else.
const PathPrefix = "/gates/"

const (
	// apiTimeout bounds one pass through the API for one page view or answer.
	apiTimeout = 30 * time.Second

	// maxFormBytes bounds a submitted answer. The form carries a decision and a
	// short comment; anything larger is not from this page.
	maxFormBytes = 16 << 10

	// MaxCommentBytes bounds the free-text comment delivered with an answer.
	MaxCommentBytes = 2000

	// maxDetailBytes bounds the API's own explanation shown on a refusal.
	maxDetailBytes = 500
)

// Path is the address of a gate's page: the one function that spells it, so a
// notification, an email and a CLI all print the same link.
//
// The workflow id and the signal name are the gate's address, the same pair
// `flow signal` takes. The result is a path; a caller joins it to the base URL
// the deployment is served at.
func Path(workflowID, signalName string) string {
	return PathPrefix + url.PathEscape(workflowID) + "/" + url.PathEscape(signalName)
}

// Handler serves the gate page. Build one with [New].
type Handler struct {
	client      flowstatev1connect.WorkflowServiceClient
	credentials func(*http.Request) string
	logger      *slog.Logger
	login       *Login
	mux         *http.ServeMux
}

// Option configures a [Handler].
type Option func(*Handler)

// WithCredentials sets how a request's credential is found. The function returns
// the full Authorization header value to present to the API ("Bearer ..."), or
// the empty string when the request carries none; the API then answers 401.
//
// The default forwards the request's own Authorization header. A deployment
// that signs people in replaces it with a function that reads its session; the
// credential still reaches the API, which is where it is verified.
func WithCredentials(credentials func(*http.Request) string) Option {
	return func(h *Handler) { h.credentials = credentials }
}

// WithLogger sets where the operator's account of a failure goes. Nothing the
// API said about an unexpected failure is shown to a visitor; this is where it
// lands.
func WithLogger(logger *slog.Logger) Option {
	return func(h *Handler) { h.logger = logger }
}

// WithLogin lets a visitor without a credential sign in with the identity
// provider l is configured for. The page then serves [LoginPath],
// [CallbackPath] and [LogoutPath], a GET that the API refuses as
// unauthenticated sends the visitor to sign in, and the session the sign-in
// sets supplies the credential when the request carries no Authorization header
// of its own.
func WithLogin(l *Login) Option {
	return func(h *Handler) { h.login = l }
}

// New returns the gate page for the API that api serves.
//
// api is the authenticated Connect handler of the deployment, the one that
// answers `/flowstate.v1.WorkflowService/...`: the page calls it in process, so a
// request is verified, authorized and audited by the same code a remote client's
// is, and an api that is not authenticated makes the page not authenticated.
func New(api http.Handler, opts ...Option) *Handler {
	client := flowstatev1connect.NewWorkflowServiceClient(
		&http.Client{Transport: loopback{handler: api}},
		"http://gates.invalid",
	)

	return newHandler(client, opts...)
}

func newHandler(client flowstatev1connect.WorkflowServiceClient, opts ...Option) *Handler {
	h := &Handler{
		client:      client,
		credentials: func(r *http.Request) string { return r.Header.Get("Authorization") },
		logger:      slog.New(slog.DiscardHandler),
		mux:         http.NewServeMux(),
	}
	for _, opt := range opts {
		opt(h)
	}

	if h.login != nil {
		base := h.credentials
		h.credentials = func(r *http.Request) string {
			if credential := base(r); credential != "" {
				return credential
			}

			return h.login.credential(r)
		}
		h.mux.HandleFunc("GET "+LoginPath, h.begin)
		h.mux.HandleFunc("GET "+CallbackPath, h.callback)
		h.mux.HandleFunc("POST "+LogoutPath, h.logout)
	}

	h.mux.HandleFunc("GET "+PathPrefix+"{workflow}/{signal}", h.show)
	h.mux.HandleFunc("POST "+PathPrefix+"{workflow}/{signal}", h.answer)
	h.mux.HandleFunc(PathPrefix, h.notFound)

	return h
}

// ServeHTTP implements [http.Handler].
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	h.mux.ServeHTTP(w, r)
}

// gate is one pending gate as the page shows it.
type gate struct {
	WorkflowID string
	RunID      string
	Signal     string
	Step       string
	Prompt     string
	Deadline   time.Time
	Starter    string
	Action     string
}

// call returns the context a request to the API runs in: the browser's request
// for the loopback transport, bounded in time.
func (h *Handler) call(r *http.Request) (context.Context, context.CancelFunc) {
	ctx := context.WithValue(r.Context(), outerRequest{}, r)

	return context.WithTimeout(ctx, apiTimeout)
}

// authorize presents the request's credential to an API call.
func (h *Handler) authorize(r *http.Request, header http.Header) {
	if credential := h.credentials(r); credential != "" {
		header.Set("Authorization", credential)
	}
}

// lookup reads the run and finds the open gate the request addresses. A run that
// is gone, finished, not the caller's, or not waiting on that signal is one
// answer, [errGateNotOpen]: telling them apart would tell a caller which runs
// exist in a tenant it cannot read.
func (h *Handler) lookup(ctx context.Context, r *http.Request, workflowID, signal string) (*gate, error) {
	req := connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID})
	h.authorize(r, req.Header())

	resp, err := h.client.Get(ctx, req)
	if err != nil {
		if connect.CodeOf(err) == connect.CodeNotFound {
			return nil, errGateNotOpen
		}

		return nil, err
	}

	run := resp.Msg
	if run.GetStatus() != v1.RunResponse_STATUS_RUNNING {
		return nil, errGateNotOpen
	}

	for _, wait := range run.GetProgress().GetPendingWaits() {
		if wait.GetSignalName() != signal {
			continue
		}

		g := &gate{
			WorkflowID: run.GetWorkflowId(),
			RunID:      run.GetRunId(),
			Signal:     signal,
			Step:       wait.GetStepId(),
			Prompt:     v1.WaitPromptDescription(wait),
			Starter:    run.GetStarter(),
			Action:     Path(workflowID, signal),
		}
		if wait.Deadline != nil {
			g.Deadline = wait.GetDeadline().AsTime().UTC()
		}

		return g, nil
	}

	// Get reports at most v1.MaxPendingWaits gates, so a run that says it left
	// some out has not shown that this one is closed.
	if run.GetProgress().GetPendingWaitsTruncated() {
		return nil, errLookupIncomplete
	}

	return nil, errGateNotOpen
}

var (
	errGateNotOpen      = errors.New("gates: no open gate")
	errLookupIncomplete = errors.New("gates: the run holds more gates than one answer lists")
)

func (h *Handler) show(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := h.call(r)
	defer cancel()

	g, err := h.lookup(ctx, r, r.PathValue("workflow"), r.PathValue("signal"))
	if err != nil {
		h.refuse(w, r, err)
		return
	}

	render(w, http.StatusOK, gatePage, g)
}

// answer delivers the decision a person made.
func (h *Handler) answer(w http.ResponseWriter, r *http.Request) {
	// Before anything is read or sent: a request the browser does not vouch for
	// as made from this page is not an answer, whatever it carries.
	if !fromThisPage(r) {
		render(w, http.StatusForbidden, noticePage, notice{
			Title:  "This answer did not come from the gate page",
			Detail: "Open the gate from its link and answer it there. A script or another site cannot answer on your behalf.",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxFormBytes)
	if err := r.ParseForm(); err != nil {
		render(w, http.StatusBadRequest, noticePage, notice{Title: "That answer could not be read"})
		return
	}

	var approved bool
	switch r.PostForm.Get("decision") {
	case "approve":
		approved = true
	case "deny":
	default:
		render(w, http.StatusBadRequest, noticePage, notice{Title: "Choose approve or deny"})
		return
	}

	comment := strings.TrimSpace(r.PostForm.Get("comment"))
	if !utf8.ValidString(comment) || len(comment) > MaxCommentBytes {
		render(w, http.StatusBadRequest, noticePage, notice{
			Title:  "That comment is too long",
			Detail: "Keep it under 2000 bytes of text.",
		})
		return
	}

	ctx, cancel := h.call(r)
	defer cancel()

	workflowID, signal := r.PathValue("workflow"), r.PathValue("signal")

	// Read again at the moment of answering, so a gate somebody else answered
	// while this page was open says so, rather than this answer being buffered
	// for whatever next waits on the same name.
	g, err := h.lookup(ctx, r, workflowID, signal)
	if err != nil {
		h.refuse(w, r, err)
		return
	}

	payload := map[string]any{"approved": approved}
	if comment != "" {
		payload["comment"] = comment
	}
	named := make(map[string]*v1.Value, len(payload))
	for key, value := range payload {
		named[key] = v1.NewValue(value)
	}

	req := connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		// The run the gate was read on, so a workflow id that has moved on to
		// another run between the read and the answer is refused, not answered.
		RunId:   g.RunID,
		Name:    signal,
		Payload: &v1.Node_Outputs{NamedValues: named},
	})
	h.authorize(r, req.Header())

	if _, err := h.client.Signal(ctx, req); err != nil {
		h.refuse(w, r, err)
		return
	}

	verdict := "Denied"
	if approved {
		verdict = "Approved"
	}
	render(w, http.StatusOK, noticePage, notice{
		Title:  verdict + ": your answer was delivered",
		Detail: "The run continues from here. You can close this page.",
		Gate:   g,
	})
}

func (h *Handler) notFound(w http.ResponseWriter, r *http.Request) {
	render(w, http.StatusNotFound, noticePage, notice{Title: "Nothing here"})
}

// refuse answers a failed call with the page and status its code deserves, and
// shows a visitor only what they are entitled to read: the API's own words for a
// refusal addressed to them, and nothing for a failure that is the operator's.
func (h *Handler) refuse(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, errLookupIncomplete) {
		render(w, http.StatusServiceUnavailable, noticePage, notice{
			Title:  "This gate could not be looked up",
			Detail: "The run is waiting on more gates than this page can list. Answer it with the CLI (flow signal) or the API.",
		})
		return
	}

	if errors.Is(err, errGateNotOpen) {
		render(w, http.StatusNotFound, noticePage, notice{
			Title:  "This gate is not open",
			Detail: "It was already answered, it timed out, the run ended, or you cannot see it.",
		})
		return
	}

	switch connect.CodeOf(err) {
	case connect.CodeUnauthenticated:
		if h.login != nil && h.signIn(w, r) {
			return
		}
		w.Header().Set("WWW-Authenticate", "Bearer")
		render(w, http.StatusUnauthorized, noticePage, notice{
			Title:  "Sign in to answer this gate",
			Detail: "Your request carried no valid credential.",
		})
	case connect.CodePermissionDenied:
		render(w, http.StatusForbidden, noticePage, notice{
			Title:  "You may not answer this gate",
			Detail: detail(err),
		})
	case connect.CodeInvalidArgument, connect.CodeFailedPrecondition:
		render(w, http.StatusBadRequest, noticePage, notice{
			Title:  "That request was refused",
			Detail: detail(err),
		})
	case connect.CodeResourceExhausted:
		render(w, http.StatusTooManyRequests, noticePage, notice{Title: "Too many requests; try again shortly"})
	default:
		h.logger.ErrorContext(r.Context(), "gate page: API call failed",
			"code", connect.CodeOf(err).String(), "error", oneLine(err.Error()))
		render(w, http.StatusBadGateway, noticePage, notice{
			Title:  "The server could not complete that",
			Detail: "Nothing was changed. Try again, and tell an operator if it keeps happening.",
		})
	}
}

// signIn answers an unauthenticated request when sign-in is configured, and
// reports whether it did. A GET that presented nothing is sent to sign in and
// back. One that presented a session the API rejected is not: sending it again
// would loop, so it gets a page that says so, the dead session cleared, and a
// link to start over.
func (h *Handler) signIn(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}

	target := LoginPath + "?next=" + url.QueryEscape(r.URL.Path)
	if !safeNext(r.URL.Path) {
		target = LoginPath
	}

	// A request that carries its own Authorization header is answered on that
	// header's merits, whatever cookies ride along.
	if r.Header.Get("Authorization") != "" {
		return false
	}

	if _, err := r.Cookie(sessionCookieName); err == nil {
		http.SetCookie(w, clearing(sessionCookieName))
		render(w, http.StatusUnauthorized, noticePage, notice{
			Title:  "Your sign-in is no longer valid",
			Detail: "It expired or the server did not accept it.",
			SignIn: target,
		})

		return true
	}
	http.Redirect(w, r, target, http.StatusSeeOther)

	return true
}

// detail is the API's own explanation of a refusal, without the code prefix and
// bounded, because a policy message can quote a configured rule.
func detail(err error) string {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return ""
	}

	return textbound.Truncate(connectErr.Message(), maxDetailBytes)
}

// oneLine keeps an API error to one log line: the API can echo a value a
// visitor chose (an id that failed validation), and a line break in it would
// let that visitor write records of their own.
func oneLine(text string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(text)
}
