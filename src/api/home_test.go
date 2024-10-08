package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthcheck(t *testing.T) {
	cfg.env = "dev"
	cfg.port = 9000
	w := httptest.NewRecorder()
	r, _ := http.NewRequest(http.MethodGet, "/healthcheck", nil)
	healthcheck(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("expected status code %d, got %d", http.StatusOK, w.Code)
	}

	want := "{\n\t\"environment\": \"dev\",\n\t\"status\": \"available\",\n\t\"version\": \"1.0.0\"\n}\n"
	if w.Body.String() != want {
		t.Errorf("expected body %q, got %q", want, w.Body.String())
	}
}
