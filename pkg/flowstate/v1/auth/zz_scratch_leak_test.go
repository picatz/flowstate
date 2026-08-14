package auth_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

func TestScratchClientSecretLeak(t *testing.T) {
	const secret = "SUPERSECRET-CLIENT-SECRET"

	ex, err := auth.NewClientCredentialsExchanger(auth.ClientCredentialsConfig{
		Name:         "partner",
		TokenURL:     "https://as.example.com/token",
		ClientID:     "flowstate",
		ClientSecret: secret,
	})
	if err != nil {
		t.Fatal(err)
	}

	type holder struct{ e auth.Exchanger }
	type nested struct{ h holder }

	renders := map[string]string{
		"%v":          fmt.Sprintf("%v", ex),
		"%+v":         fmt.Sprintf("%+v", ex),
		"%#v":         fmt.Sprintf("%#v", ex),
		"%s":          fmt.Sprintf("%s", ex),
		"holder %v":   fmt.Sprintf("%v", holder{ex}),
		"holder %+v":  fmt.Sprintf("%+v", holder{ex}),
		"holder %#v":  fmt.Sprintf("%#v", holder{ex}),
		"nested %v":   fmt.Sprintf("%v", nested{holder{ex}}),
		"slice %v":    fmt.Sprintf("%v", []auth.Exchanger{ex}),
		"map %v":      fmt.Sprintf("%v", map[string]auth.Exchanger{"a": ex}),
	}

	for name, r := range renders {
		if bytes.Contains([]byte(r), []byte(secret)) {
			t.Errorf("LEAK via %s: %s", name, r)
		} else {
			t.Logf("ok %s -> %.200s", name, r)
		}
	}

	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("x", "exchanger", ex)
	if bytes.Contains(buf.Bytes(), []byte(secret)) {
		t.Errorf("LEAK via slog: %s", buf.String())
	} else {
		t.Logf("ok slog -> %.200s", buf.String())
	}

	// And a broker holding it.
	key, err := auth.GenerateSigningKey("k", "ES256")
	if err != nil {
		t.Fatal(err)
	}
	iss, err := auth.NewIssuer("https://flowstate.example.com", key)
	if err != nil {
		t.Fatal(err)
	}
	broker, err := auth.NewBroker(iss, auth.WithTarget("partner", ex))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"%v", "%+v", "%#v"} {
		r := fmt.Sprintf(v, broker)
		if bytes.Contains([]byte(r), []byte(secret)) {
			t.Errorf("LEAK broker %s: %s", v, r)
		} else {
			t.Logf("ok broker %s -> %.300s", v, r)
		}
		r2 := fmt.Sprintf(v, *broker)
		if bytes.Contains([]byte(r2), []byte(secret)) {
			t.Errorf("LEAK *broker %s: %s", v, r2)
		} else {
			t.Logf("ok *broker %s -> %.300s", v, r2)
		}
	}
}
