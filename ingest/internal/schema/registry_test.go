package schema

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSubjectForTopic(t *testing.T) {
	if got := SubjectForTopic("md.trades.v1"); got != "md.trades.v1-value" {
		t.Errorf("SubjectForTopic = %q, want md.trades.v1-value", got)
	}
}

func TestRegisterAndCacheByID(t *testing.T) {
	var registerCalls int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/versions"):
			atomic.AddInt64(&registerCalls, 1)

			var req registerRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decoding register body: %v", err)
			}
			if req.SchemaType != "AVRO" {
				t.Errorf("schemaType = %q, want AVRO", req.SchemaType)
			}
			if !strings.Contains(r.URL.Path, "md.trades.v1-value") {
				t.Errorf("registered under %q, want the -value subject", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
			_ = json.NewEncoder(w).Encode(registerResponse{ID: 77})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := New(srv.URL)
	ctx := context.Background()
	const schemaJSON = `{"type":"record","name":"T","fields":[]}`

	id, err := c.Register(ctx, "md.trades.v1-value", schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if id != 77 {
		t.Errorf("id = %d, want 77", id)
	}

	// The publish path calls this per process start, not per message — but
	// the cache is what guarantees that stays true even if a caller loops.
	for i := 0; i < 5; i++ {
		if _, err := c.Register(ctx, "md.trades.v1-value", schemaJSON); err != nil {
			t.Fatalf("Register (cached): %v", err)
		}
	}
	if n := atomic.LoadInt64(&registerCalls); n != 1 {
		t.Errorf("registry was called %d times, want 1 — the cache is not holding", n)
	}

	// Registering populates the id cache, so no round trip is needed here.
	got, err := c.SchemaByID(ctx, 77)
	if err != nil {
		t.Fatalf("SchemaByID: %v", err)
	}
	if got != schemaJSON {
		t.Errorf("SchemaByID = %q, want the registered schema", got)
	}
}

func TestSchemaByIDFetchesAndCaches(t *testing.T) {
	var fetches int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/schemas/ids/5" {
			atomic.AddInt64(&fetches, 1)
			_ = json.NewEncoder(w).Encode(schemaResponse{Schema: `{"type":"string"}`})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(srv.URL)
	for i := 0; i < 3; i++ {
		s, err := c.SchemaByID(context.Background(), 5)
		if err != nil {
			t.Fatalf("SchemaByID: %v", err)
		}
		if s != `{"type":"string"}` {
			t.Fatalf("schema = %q", s)
		}
	}
	if n := atomic.LoadInt64(&fetches); n != 1 {
		t.Errorf("fetched %d times, want 1", n)
	}
}

func TestCheckCompatibility(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    any
		want    bool
		wantErr bool
	}{
		{name: "compatible", status: 200, body: compatResponse{IsCompatible: true}, want: true},
		{name: "incompatible", status: 200, body: compatResponse{IsCompatible: false}, want: false},
		// A brand new subject 404s. That is not a failure — there is simply
		// nothing yet to be incompatible with.
		{name: "unknown subject is compatible", status: 404, body: nil, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				if tc.body != nil {
					_ = json.NewEncoder(w).Encode(tc.body)
				}
			}))
			defer srv.Close()

			got, err := New(srv.URL).CheckCompatibility(
				context.Background(), "md.trades.v1-value", `{"type":"string"}`)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("compatible = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRegistryErrorsSurfaceTheRegistryMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(errorResponse{
			ErrorCode: 409,
			Message:   "Schema being registered is incompatible with an earlier schema",
		})
	}))
	defer srv.Close()

	_, err := New(srv.URL).Register(context.Background(), "s", `{"type":"string"}`)
	if err == nil {
		t.Fatal("a 409 was swallowed")
	}
	// The registry's own wording is the useful part of this error; losing it
	// turns a five-minute fix into an afternoon.
	if !strings.Contains(err.Error(), "incompatible with an earlier schema") {
		t.Errorf("error dropped the registry message: %v", err)
	}
}

func TestNotFoundIsDistinguishable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	_, err := New(srv.URL).SchemaByID(context.Background(), 1)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want it to wrap ErrNotFound", err)
	}
}

func TestSetCompatibility(t *testing.T) {
	var gotLevel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotLevel = body["compatibility"]
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	if err := New(srv.URL).SetCompatibility(context.Background(), "s", CompatFull); err != nil {
		t.Fatalf("SetCompatibility: %v", err)
	}
	if gotLevel != "FULL" {
		t.Errorf("level = %q, want FULL", gotLevel)
	}
}

func TestTrailingSlashInBaseURL(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_ = json.NewEncoder(w).Encode(registerResponse{ID: 1})
	}))
	defer srv.Close()

	c := New(srv.URL + "/")
	if _, err := c.Register(context.Background(), "s", `{"type":"string"}`); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if strings.Contains(path, "//") {
		t.Errorf("double slash in path %q", path)
	}
}
