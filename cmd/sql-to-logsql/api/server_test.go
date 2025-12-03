package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestHandleQuerySuccess(t *testing.T) {
	srv, err := NewServer(Config{Endpoint: "http://victoria", Tables: map[string]string{"logs": "*"}})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	srv.setHTTPClient(&http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if err := req.ParseForm(); err != nil {
				t.Fatalf("failed to parse form: %v", err)
			}
			if got := req.Form.Get("query"); got != "*" {
				t.Fatalf("unexpected query sent: %q", got)
			}
			if got := req.Form.Get("start"); got != "1764770090000" {
				t.Fatalf("unexpected start sent: %q", got)
			}
			if got := req.Form.Get("end"); got != "1764856490000" {
				t.Fatalf("unexpected end sent: %q", got)
			}
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(`{"status":"ok"}`)),
				Header:     make(http.Header),
			}
			resp.Header.Set("Content-Type", "application/json")
			return resp, nil
		}),
	})

	reqBody := map[string]string{
		"sql":   "SELECT * FROM logs",
		"start": "1764770090000",
		"end":   "1764856490000",
	}
	buf, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql-to-logsql", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	var resp struct {
		LogsQL string `json:"logsql"`
		Data   string `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json response: %v", err)
	}
	if resp.LogsQL != "*" {
		t.Fatalf("unexpected LogsQL: %s", resp.LogsQL)
	}
	if resp.Data != `{"status":"ok"}` {
		t.Fatalf("unexpected victoria payload: %s", resp.Data)
	}
}

func TestHandleQueryCustomTable(t *testing.T) {
	srv, err := NewServer(Config{
		Endpoint: "http://victoria",
		Tables:   map[string]string{"logs": "*", "errors": "* | level:ERROR"},
	})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	srv.setHTTPClient(&http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if err := req.ParseForm(); err != nil {
				t.Fatalf("failed to parse form: %v", err)
			}
			if got := req.Form.Get("query"); got != "* | level:ERROR" {
				t.Fatalf("unexpected query sent: %q", got)
			}
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(`{"status":"ok"}`)),
				Header:     make(http.Header),
			}
			resp.Header.Set("Content-Type", "application/json")
			return resp, nil
		}),
	})

	reqBody := map[string]string{"sql": "SELECT * FROM errors"}
	buf, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql-to-logsql", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	var resp struct {
		LogsQL string `json:"logsql"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json response: %v", err)
	}
	if resp.LogsQL != "* | level:ERROR" {
		t.Fatalf("unexpected LogsQL: %s", resp.LogsQL)
	}
}

func TestHandleQueryTranslateError(t *testing.T) {
	srv, err := NewServer(Config{Endpoint: "http://victoria", Tables: map[string]string{"logs": "*"}})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	reqBody := map[string]string{"sql": "SELECT"}
	buf, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql-to-logsql", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rr.Code)
	}
}

func TestHandleQueryVictoriaError(t *testing.T) {
	srv, err := NewServer(Config{Endpoint: "http://victoria", Tables: map[string]string{"logs": "*"}})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	srv.setHTTPClient(&http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		resp := &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(bytes.NewBufferString("fail")),
			Header:     make(http.Header),
		}
		return resp, nil
	})})

	reqBody := map[string]string{"sql": "SELECT * FROM logs"}
	buf, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql-to-logsql", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("expected status 502, got %d", rr.Code)
	}
}

func TestHandleDescribeTable(t *testing.T) {
	srv, err := NewServer(Config{Endpoint: "http://victoria", Tables: map[string]string{"logs": "*"}})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	srv.setHTTPClient(&http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/select/logsql/field_names" {
				t.Fatalf("unexpected path: %s", req.URL.Path)
			}
			if err := req.ParseForm(); err != nil {
				t.Fatalf("failed to parse form: %v", err)
			}
			if got := req.Form.Get("query"); got != "*" {
				t.Fatalf("unexpected query sent: %q", got)
			}
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(`{"values":[{"value":"field1","hits":1000},{"value":"field2","hits":1}]}`)),
				Header:     make(http.Header),
			}
			resp.Header.Set("Content-Type", "application/json")
			return resp, nil
		}),
	})

	reqBody := map[string]string{"sql": "DESCRIBE TABLE logs"}
	buf, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql-to-logsql", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	var resp struct {
		LogsQL string `json:"logsql"`
		Data   string `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json response: %v", err)
	}
	if resp.LogsQL != "*" {
		t.Fatalf("unexpected LogsQL: %s", resp.LogsQL)
	}
	if resp.Data != `{"field_name":"field1","hits":1000}
{"field_name":"field2","hits":1}
` {
		t.Fatalf("unexpected describe payload: %s", resp.Data)
	}
}

func TestHandleDescribeView(t *testing.T) {
	dir := t.TempDir()
	viewPath := filepath.Join(dir, "errors.logsql")
	if err := os.WriteFile(viewPath, []byte("* | level:ERROR\n"), 0o644); err != nil {
		t.Fatalf("failed to write view: %v", err)
	}

	srv, err := NewServer(Config{Endpoint: "http://victoria", ViewsDir: dir})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	srv.setHTTPClient(&http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/select/logsql/field_names" {
				t.Fatalf("unexpected path: %s", req.URL.Path)
			}
			if err := req.ParseForm(); err != nil {
				t.Fatalf("failed to parse form: %v", err)
			}
			if got := req.Form.Get("query"); got != "* | level:ERROR" {
				t.Fatalf("unexpected query sent: %q", got)
			}
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(`{"values":[{"value":"field1","hits":1000},{"value":"field2","hits":1}]}`)),
				Header:     make(http.Header),
			}
			resp.Header.Set("Content-Type", "application/json")
			return resp, nil
		}),
	})

	reqBody := map[string]string{"sql": "DESCRIBE VIEW errors"}
	buf, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql-to-logsql", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	var resp struct {
		LogsQL string `json:"logsql"`
		Data   string `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json response: %v", err)
	}
	if resp.LogsQL != "* | level:ERROR" {
		t.Fatalf("unexpected LogsQL: %s", resp.LogsQL)
	}
	if resp.Data != `{"field_name":"field1","hits":1000}
{"field_name":"field2","hits":1}
` {
		t.Fatalf("unexpected describe payload: %s", resp.Data)
	}
}

func TestHandleShowTables(t *testing.T) {
	srv, err := NewServer(Config{Tables: map[string]string{"logs": "*", "errors": "* | level:ERROR"}})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	srv.setHTTPClient(&http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("unexpected HTTP call for SHOW TABLES: %s", req.URL)
		return nil, nil
	})})

	reqBody := map[string]string{"sql": "SHOW TABLES"}
	buf, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql-to-logsql", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	var resp struct {
		LogsQL string `json:"logsql"`
		Data   string `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json response: %v", err)
	}
	if resp.LogsQL != "" {
		t.Fatalf("expected empty LogsQL, got %q", resp.LogsQL)
	}
	const expected = "{\"table_name\":\"errors\",\"query\":\"* | level:ERROR\"}\n{\"table_name\":\"logs\",\"query\":\"*\"}\n"
	if resp.Data != expected {
		t.Fatalf("unexpected SHOW TABLES payload:\nexpected: %s\nactual: %s", expected, resp.Data)
	}
}

func TestHandleShowViews(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "errors.logsql"), []byte("* | level:ERROR\n"), 0o644); err != nil {
		t.Fatalf("failed to write view: %v", err)
	}
	srv, err := NewServer(Config{ViewsDir: dir})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	srv.setHTTPClient(&http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("unexpected HTTP call for SHOW VIEWS: %s", req.URL)
		return nil, nil
	})})

	reqBody := map[string]string{"sql": "SHOW VIEWS"}
	buf, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql-to-logsql", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	var resp struct {
		LogsQL string `json:"logsql"`
		Data   string `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json response: %v", err)
	}
	if resp.LogsQL != "" {
		t.Fatalf("expected empty LogsQL, got %q", resp.LogsQL)
	}
	const expected = "{\"view_name\":\"errors\",\"query\":\"* | level:ERROR\"}\n"
	if resp.Data != expected {
		t.Fatalf("unexpected SHOW VIEWS payload:\nexpected: %s\nactual: %s", expected, resp.Data)
	}
}

func TestHandleDescribeWithoutEndpoint(t *testing.T) {
	srv, err := NewServer(Config{Tables: map[string]string{"logs": "*"}})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	reqBody := map[string]string{"sql": "DESCRIBE TABLE logs"}
	buf, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql-to-logsql", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
}

func TestHandleIndex(t *testing.T) {
	srv, err := NewServer(Config{Endpoint: "http://victoria"})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("expected text/html content type, got %s", ct)
	}
}

func TestHandleHealth(t *testing.T) {
	srv, err := NewServer(Config{Tables: map[string]string{"logs": "*"}})
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	var resp map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json response: %v", err)
	}
	if resp["status"] != "ok" {
		t.Fatalf("unexpected health status: %v", resp["status"])
	}
}
