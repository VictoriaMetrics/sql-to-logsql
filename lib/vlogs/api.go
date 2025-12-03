package vlogs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/VictoriaMetrics/sql-to-logsql/lib/logsql"
)

type EndpointConfig struct {
	Endpoint    string
	BearerToken string
}

type QueryMode string

const (
	QueryModeTranslate        QueryMode = "translate"
	QueryModeTranslateAndLoad QueryMode = "query"
)

type RequestParams struct {
	EndpointConfig
	Start    string
	End      string
	ExecMode string
}

type API struct {
	ec     EndpointConfig
	limit  uint32
	client *http.Client
}

func NewVLogsAPI(ec EndpointConfig, limit uint32) *API {
	return &API{
		ec:    ec,
		limit: limit,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

func (a *API) Execute(ctx context.Context, si *logsql.StatementInfo, params RequestParams) ([]byte, error) {
	if a.ec.Endpoint != "" && params.Endpoint != "" {
		return nil, &APIError{
			Code:    http.StatusBadRequest,
			Message: "endpoint can be set either in config or in request, not both",
		}
	}
	recParams := params
	if recParams.Endpoint == "" {
		recParams.Endpoint = a.ec.Endpoint
		recParams.BearerToken = a.ec.BearerToken
	}

	switch si.Kind {
	case logsql.StatementTypeSelect:
		if recParams.Endpoint == "" || recParams.ExecMode == "translate" {
			return nil, nil
		}
		return a.Query(ctx, si.LogsQL, recParams)
	case logsql.StatementTypeDescribe:
		if recParams.Endpoint == "" || recParams.ExecMode == "translate" {
			return nil, nil
		}
		return a.GetFieldNames(ctx, si.LogsQL, recParams)
	case logsql.StatementTypeCreateView, logsql.StatementTypeDropView:
		return []byte(si.Data), nil
	case logsql.StatementTypeShowTables, logsql.StatementTypeShowViews:
		return []byte(si.Data), nil
	default:
		return nil, &APIError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("vlogs: unsupported statement type %s", si.Kind),
		}
	}
}

func (a *API) SetHTTPClient(client *http.Client) {
	a.client = client
}

func (a *API) Query(ctx context.Context, logsQL string, params RequestParams) ([]byte, error) {
	if params.Endpoint == "" {
		return nil, &APIError{
			Code:    http.StatusBadRequest,
			Message: "endpoint is required for this statement",
		}
	}
	reqURL, err := url.Parse(params.Endpoint)
	if err != nil {
		return nil, &APIError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("invalid endpoint URL: %v", params.Endpoint),
			Err:     err,
		}
	}
	reqURL = reqURL.JoinPath("/select/logsql/query")
	form := url.Values{}
	form.Set("query", logsQL)
	form.Set("limit", fmt.Sprintf("%d", a.limit))
	if params.Start != "" {
		form.Set("start", params.Start)
	}
	if params.End != "" {
		form.Set("end", params.End)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return nil, &APIError{
			Code:    http.StatusBadGateway,
			Message: "failed to create request",
			Err:     err,
		}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if params.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+params.BearerToken)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, &APIError{
			Code:    http.StatusBadGateway,
			Message: "failed to execute request",
			Err:     err,
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{
			Code:    http.StatusBadGateway,
			Message: "failed to read response body",
			Err:     err,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return nil, &APIError{
			Code:    http.StatusBadGateway,
			Message: fmt.Sprintf("status %d: %s", resp.StatusCode, msg),
		}
	}
	return body, nil
}

type FieldNamesResponse struct {
	Values []FieldNamesValue `json:"values"`
}

type FieldNamesValue struct {
	Value string `json:"value"`
	Hits  uint64 `json:"hits"`
}

func (a *API) GetFieldNames(ctx context.Context, logsQL string, params RequestParams) ([]byte, error) {
	if params.Endpoint == "" {
		return nil, &APIError{
			Code:    http.StatusBadRequest,
			Message: "endpoint is required for this statement",
		}
	}
	reqURL, err := url.Parse(params.Endpoint)
	if err != nil {
		return nil, &APIError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("invalid endpoint URL: %v", params.Endpoint),
			Err:     err,
		}
	}
	reqURL = reqURL.JoinPath("/select/logsql/field_names")
	form := url.Values{}
	form.Set("query", logsQL)
	if params.Start != "" {
		form.Set("start", params.Start)
	}
	if params.End != "" {
		form.Set("end", params.End)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return nil, &APIError{
			Code:    http.StatusBadGateway,
			Message: "failed to create request",
			Err:     err,
		}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if params.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+params.BearerToken)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, &APIError{
			Code:    http.StatusBadGateway,
			Message: "failed to execute request",
			Err:     err,
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{
			Code:    http.StatusBadGateway,
			Message: "failed to read response body",
			Err:     err,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return nil, &APIError{
			Code:    http.StatusBadGateway,
			Message: fmt.Sprintf("status %d: %s", resp.StatusCode, msg),
		}
	}

	respData := FieldNamesResponse{}
	if err = json.Unmarshal(body, &respData); err != nil {
		return nil, &APIError{
			Code:    http.StatusBadGateway,
			Message: "failed to parse response body",
			Err:     err,
		}
	}

	result := strings.Builder{}
	for _, v := range respData.Values {
		row, err := json.Marshal(map[string]any{
			"field_name": v.Value,
			"hits":       v.Hits,
		})
		if err != nil {
			return nil, &APIError{
				Code:    http.StatusBadGateway,
				Message: "failed to marshal response body",
				Err:     err,
			}
		}
		result.Write(row)
		result.Write([]byte{'\n'})
	}
	return []byte(result.String()), nil
}
