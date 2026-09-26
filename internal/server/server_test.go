package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthzHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	healthzHandler(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"status":"healthy"}`, rec.Body.String())
}

func TestNewReadyzHandlerSuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]string{"version": "1.2.3"},
		})
		require.NoError(t, err)
	}))
	defer ts.Close()

	handler := newReadyzHandler(ts.URL, time.Second)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"status":"OK","version":"1.2.3"}`, rec.Body.String())
}

func TestNewReadyzHandlerTimesOutOnSlowDagster(t *testing.T) {
	unblock := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-unblock
	}))
	// unblock must be closed before ts.Close(), since Close() waits for the
	// in-flight handler above to return.
	defer ts.Close()
	defer close(unblock)

	handler := newReadyzHandler(ts.URL, 50*time.Millisecond)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Less(t, elapsed, 2*time.Second, "handler should have been bounded by its own timeout, not hung on a slow Dagster")
}

func TestNewReadyzHandlerFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	handler := newReadyzHandler(ts.URL, time.Second)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "NOT_READY", body["status"])
	assert.NotEmpty(t, body["error"])
}

func TestNewHTTPServerSetsTimeouts(t *testing.T) {
	readyzTimeout := 10 * time.Second
	srv := newHTTPServer(http.NewServeMux(), readyzTimeout)

	// Without ReadHeaderTimeout, a client that trickles its headers holds a
	// connection (and a goroutine) open indefinitely.
	assert.Positive(t, srv.ReadHeaderTimeout)
	assert.Positive(t, srv.ReadTimeout)
	assert.Positive(t, srv.IdleTimeout)
	// /readyz may legitimately spend the whole readyzTimeout on its GraphQL
	// call; the response must still be writable after that.
	assert.Greater(t, srv.WriteTimeout, readyzTimeout)
}

func TestServeReturnsErrorWhenServingFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// A listener that's already closed makes Serve fail immediately, standing
	// in for any failure after startup.
	require.NoError(t, ln.Close())

	done := make(chan error, 1)
	go func() { done <- serve(t.Context(), newHTTPServer(http.NewServeMux(), time.Second), ln) }()

	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("serve should have returned the Serve error instead of blocking until ctx is cancelled")
	}
}

func TestServeShutsDownCleanlyOnCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, newHTTPServer(mux, time.Second), ln) }()

	resp, err := http.Get("http://" + ln.Addr().String() + "/healthz")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("serve should have returned after ctx was cancelled")
	}
}
