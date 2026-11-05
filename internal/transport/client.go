package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/m-pedro/godistkv/internal/version"
)

// Client sends internal replica RPC requests to peer nodes in the cluster.
type Client struct {
	httpClient *http.Client
}

func NewClient(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	return &Client{
		httpClient: &http.Client{Timeout: timeout},
	}
}

// PutReplica sends a VersionedRecord to a peer replica's /internal/kv endpoint.
func (c *Client) PutReplica(ctx context.Context, baseURL string, rec version.VersionedRecord) (version.VersionedRecord, error) {
	body, err := json.Marshal(rec)
	if err != nil {
		return version.VersionedRecord{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, baseURL+"/internal/kv", bytes.NewReader(body))
	if err != nil {
		return version.VersionedRecord{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return version.VersionedRecord{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return version.VersionedRecord{}, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, baseURL)
	}

	var stored version.VersionedRecord
	if err := json.NewDecoder(resp.Body).Decode(&stored); err != nil {
		return version.VersionedRecord{}, err
	}
	return stored, nil
}

// GetReplica fetches a VersionedRecord from a peer replica's /internal/kv endpoint.
// Returns (rec, true, nil) if found, or (zero, false, nil) on 404 Not Found.
func (c *Client) GetReplica(ctx context.Context, baseURL string, key string) (version.VersionedRecord, bool, error) {
	endpoint := fmt.Sprintf("%s/internal/kv?key=%s", baseURL, url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return version.VersionedRecord{}, false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return version.VersionedRecord{}, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return version.VersionedRecord{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return version.VersionedRecord{}, false, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, baseURL)
	}

	var rec version.VersionedRecord
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return version.VersionedRecord{}, false, err
	}
	return rec, true, nil
}

// Ping checks if a peer node is reachable.
func (c *Client) Ping(ctx context.Context, baseURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/internal/ping", nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping failed with status %d", resp.StatusCode)
	}
	return nil
}
