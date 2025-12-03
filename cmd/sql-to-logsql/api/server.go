package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/sql-to-logsql/cmd/sql-to-logsql/web"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/logsql"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/sql/lexer"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/sql/parser"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/store"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/store/tablestore"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/store/viewstore"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/vlogs"
)

type Config struct {
	ListenAddr  string            `json:"listenAddr"`
	Endpoint    string            `json:"endpoint"`
	BearerToken string            `json:"bearerToken"`
	Tables      map[string]string `json:"tables"`
	ViewsDir    string            `json:"viewsDir"`
	Limit       uint32            `json:"limit"`
}

type Server struct {
	api *vlogs.API
	mux *http.ServeMux
	sp  *store.Provider
}

func NewServer(cfg Config) (*Server, error) {
	serverCfg := cfg
	serverCfg.BearerToken = strings.TrimSpace(serverCfg.BearerToken)
	serverCfg.Endpoint = strings.TrimSpace(serverCfg.Endpoint)
	if serverCfg.Endpoint != "" {
		if _, err := url.Parse(serverCfg.Endpoint); err != nil {
			return nil, fmt.Errorf("invalid endpoint URL: %w", err)
		}
	}
	serverCfg.ViewsDir = strings.TrimSpace(serverCfg.ViewsDir)

	tableStore, err := tablestore.NewTableStore(serverCfg.Tables)
	if err != nil {
		return nil, fmt.Errorf("failed to create table store: %w", err)
	}
	viewStore, err := viewstore.NewViewStore(serverCfg.ViewsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create view store: %w", err)
	}
	sp := store.NewStoreProvider(tableStore, viewStore)

	srv := &Server{
		mux: http.NewServeMux(),
		sp:  sp,
		api: vlogs.NewVLogsAPI(
			vlogs.EndpointConfig{
				Endpoint:    serverCfg.Endpoint,
				BearerToken: serverCfg.BearerToken,
			},
			serverCfg.Limit,
		),
	}
	srv.mux.HandleFunc("/healthz", withSecurityHeaders(srv.handleHealth))
	srv.mux.HandleFunc("/api/v1/sql-to-logsql", withSecurityHeaders(srv.handleQuery))
	srv.mux.HandleFunc("/api/v1/config", withSecurityHeaders(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"endpoint": serverCfg.Endpoint, "limit": serverCfg.Limit})
	}))
	srv.mux.HandleFunc("/", withSecurityHeaders(srv.handleStatic))
	return srv, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) setHTTPClient(client *http.Client) {
	s.api.SetHTTPClient(client)
}

// withSecurityHeaders middleware adds security headers to responses
func withSecurityHeaders(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		next(w, r)
	}
}

type queryRequest struct {
	SQL         string `json:"sql"`
	Endpoint    string `json:"endpoint,omitempty"`
	BearerToken string `json:"bearerToken,omitempty"`
	Start       string `json:"start,omitempty"`
	End         string `json:"end,omitempty"`
	ExecMode    string `json:"execMode,omitempty"`
}

type queryResponse struct {
	LogsQL string `json:"logsql"`
	Data   string `json:"data,omitempty"`
	Error  string `json:"error,omitempty"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("ERROR: failed to decode request: %v", err)
		writeJSON(w, http.StatusBadRequest, queryResponse{Error: "invalid request payload"})
		return
	}

	sqlText := strings.TrimSpace(req.SQL)
	if sqlText == "" {
		writeJSON(w, http.StatusBadRequest, queryResponse{Error: "sql query is required"})
		return
	}
	execMode := strings.TrimSpace(strings.ToLower(req.ExecMode))
	if execMode != "" && execMode != "translate" && execMode != "query" {
		writeJSON(w, http.StatusBadRequest, queryResponse{Error: "invalid execMode: possible values are translate and query"})
		return
	}
	start := strings.TrimSpace(req.Start)
	end := strings.TrimSpace(req.End)

	statement, err := processQuery(sqlText, s.sp)
	if err != nil {
		log.Printf("ERROR: query processing failed: %v", err)
		var ae *vlogs.APIError
		var te *logsql.TranslationError
		var ve *viewstore.StoreError
		var se *parser.SyntaxError
		if errors.As(err, &ae) {
			writeJSON(w, ae.Code, queryResponse{Error: ae.Message})
		} else if errors.As(err, &te) {
			writeJSON(w, te.Code, queryResponse{Error: te.Message})
		} else if errors.As(err, &ve) {
			writeJSON(w, ve.Code, queryResponse{Error: ve.Message})
		} else if errors.As(err, &se) {
			writeJSON(w, http.StatusBadRequest, queryResponse{Error: err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, queryResponse{Error: "query processing failed"})
		}
		return
	}

	resp := queryResponse{LogsQL: statement.LogsQL}
	data, err := s.api.Execute(r.Context(), statement, vlogs.RequestParams{
		EndpointConfig: vlogs.EndpointConfig{
			Endpoint:    req.Endpoint,
			BearerToken: req.BearerToken,
		},
		Start:    start,
		End:      end,
		ExecMode: execMode,
	})
	if err != nil {
		log.Printf("ERROR: query execution failed: %v", err)
		var ae *vlogs.APIError
		var te *logsql.TranslationError
		var ve *viewstore.StoreError
		var se *parser.SyntaxError
		if errors.As(err, &ae) {
			writeJSON(w, ae.Code, queryResponse{Error: ae.Message})
		} else if errors.As(err, &te) {
			writeJSON(w, te.Code, queryResponse{Error: te.Message})
		} else if errors.As(err, &ve) {
			writeJSON(w, ve.Code, queryResponse{Error: ve.Message})
		} else if errors.As(err, &se) {
			writeJSON(w, http.StatusBadRequest, queryResponse{Error: err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, queryResponse{Error: "query execution failed"})
		}
		return
	}
	resp.Data = string(data)
	writeJSON(w, http.StatusOK, resp)
}

func processQuery(sql string, sp *store.Provider) (*logsql.StatementInfo, error) {
	l := lexer.New(sql)
	p := parser.New(l)
	stmt := p.ParseStatement()
	if stmt == nil {
		return nil, errors.New("no statement parsed")
	}
	if perrs := p.Errors(); len(perrs) > 0 {
		return nil, fmt.Errorf("parse errors: %w", errors.Join(perrs...))
	}
	result, err := logsql.GetStatementInfo(stmt, sp)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("ERROR: failed to encode JSON response: %v", err)
	}
}

var (
	indexOnce  sync.Once
	indexBytes []byte
	indexErr   error
	uiFS       = web.DistFS()
)

func loadIndex() ([]byte, error) {
	indexOnce.Do(func() {
		indexBytes, indexErr = web.ReadFile("index.html")
	})
	return indexBytes, indexErr
}

func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cleaned := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
	if cleaned == "" || cleaned == "index.html" {
		index, err := loadIndex()
		if err != nil {
			http.Error(w, "ui not available", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
		return
	}

	file, err := uiFS.Open(cleaned)
	if err != nil {
		serveIndexFallback(w, r)
		return
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil || info.IsDir() {
		serveIndexFallback(w, r)
		return
	}

	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "failed to read asset", http.StatusInternalServerError)
		return
	}

	if ct := mime.TypeByExtension(path.Ext(cleaned)); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	http.ServeContent(w, r, cleaned, info.ModTime(), bytes.NewReader(data))
}

func serveIndexFallback(w http.ResponseWriter, r *http.Request) {
	index, err := loadIndex()
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(index)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
