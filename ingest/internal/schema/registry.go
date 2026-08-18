// Package schema is a small Confluent Schema Registry client.
//
// Only the endpoints this project needs are implemented, deliberately: a
// registry client is the kind of dependency that is easier to own 200 lines
// of than to debug through a vendor SDK, and owning it is the point — the
// wire format below is the part interviewers ask about.
package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Compatibility levels understood by the registry.
const (
	CompatBackward = "BACKWARD"
	CompatForward  = "FORWARD"
	CompatFull     = "FULL"
	CompatNone     = "NONE"
)

// ErrNotFound is returned for 404s from the registry.
var ErrNotFound = errors.New("schema registry: not found")

// Client talks to a Confluent-compatible Schema Registry.
//
// Registered ids are cached forever (ids are immutable in the registry), so
// the hot publish path never makes a network call.
type Client struct {
	baseURL string
	http    *http.Client

	mu        sync.RWMutex
	idByKey   map[string]int // subject + "\x00" + canonicalized schema -> id
	schemaMap map[int]string // id -> schema JSON
}

// New returns a Client for a registry base URL such as http://localhost:8081.
func New(baseURL string) *Client {
	return &Client{
		baseURL:   strings.TrimRight(baseURL, "/"),
		http:      &http.Client{Timeout: 10 * time.Second},
		idByKey:   make(map[string]int),
		schemaMap: make(map[int]string),
	}
}

// WithHTTPClient overrides the HTTP client, which is how the tests point the
// client at an httptest server.
func (c *Client) WithHTTPClient(h *http.Client) *Client {
	c.http = h
	return c
}

// SubjectForTopic returns the TopicNameStrategy subject for a topic's value.
// This is the registry's default strategy and the one the Flink and serving
// tiers assume.
func SubjectForTopic(topic string) string { return topic + "-value" }

type registerRequest struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType,omitempty"`
}

type registerResponse struct {
	ID int `json:"id"`
}

type schemaResponse struct {
	Schema string `json:"schema"`
}

type compatResponse struct {
	IsCompatible bool `json:"is_compatible"`
}

type errorResponse struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

// Register registers a schema under a subject and returns its global id. The
// registry is idempotent: registering an identical schema returns the
// existing id rather than creating a new version.
func (c *Client) Register(ctx context.Context, subject, schemaJSON string) (int, error) {
	key := cacheKey(subject, schemaJSON)

	c.mu.RLock()
	id, ok := c.idByKey[key]
	c.mu.RUnlock()
	if ok {
		return id, nil
	}

	body, err := json.Marshal(registerRequest{Schema: schemaJSON, SchemaType: "AVRO"})
	if err != nil {
		return 0, err
	}

	var out registerResponse
	url := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, subject)
	if err := c.do(ctx, http.MethodPost, url, body, &out); err != nil {
		return 0, fmt.Errorf("register %q: %w", subject, err)
	}

	c.mu.Lock()
	c.idByKey[key] = out.ID
	c.schemaMap[out.ID] = schemaJSON
	c.mu.Unlock()

	return out.ID, nil
}

// SchemaByID fetches a schema by its global id, caching the result.
func (c *Client) SchemaByID(ctx context.Context, id int) (string, error) {
	c.mu.RLock()
	s, ok := c.schemaMap[id]
	c.mu.RUnlock()
	if ok {
		return s, nil
	}

	var out schemaResponse
	url := fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id)
	if err := c.do(ctx, http.MethodGet, url, nil, &out); err != nil {
		return "", fmt.Errorf("fetch schema id %d: %w", id, err)
	}

	c.mu.Lock()
	c.schemaMap[id] = out.Schema
	c.mu.Unlock()

	return out.Schema, nil
}

// CheckCompatibility asks the registry whether a candidate schema is
// compatible with the latest registered version of a subject.
//
// CI calls this before a deploy. That is the whole point of the registry:
// an incompatible schema should fail a pull request, not a consumer at 3am.
func (c *Client) CheckCompatibility(ctx context.Context, subject, schemaJSON string) (bool, error) {
	body, err := json.Marshal(registerRequest{Schema: schemaJSON, SchemaType: "AVRO"})
	if err != nil {
		return false, err
	}
	var out compatResponse
	url := fmt.Sprintf("%s/compatibility/subjects/%s/versions/latest", c.baseURL, subject)
	if err := c.do(ctx, http.MethodPost, url, body, &out); err != nil {
		if errors.Is(err, ErrNotFound) {
			// No prior version means nothing to be incompatible with.
			return true, nil
		}
		return false, err
	}
	return out.IsCompatible, nil
}

// SetCompatibility pins a subject's compatibility level.
func (c *Client) SetCompatibility(ctx context.Context, subject, level string) error {
	body, err := json.Marshal(map[string]string{"compatibility": level})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/config/%s", c.baseURL, subject)
	return c.do(ctx, http.MethodPut, url, body, nil)
}

func (c *Client) do(ctx context.Context, method, url string, body []byte, out any) error {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	if body != nil {
		req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e errorResponse
		if json.Unmarshal(raw, &e) == nil && e.Message != "" {
			return fmt.Errorf("registry %d (code %d): %s", resp.StatusCode, e.ErrorCode, e.Message)
		}
		return fmt.Errorf("registry %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func cacheKey(subject, schemaJSON string) string {
	return subject + "\x00" + strings.Join(strings.Fields(schemaJSON), " ")
}
