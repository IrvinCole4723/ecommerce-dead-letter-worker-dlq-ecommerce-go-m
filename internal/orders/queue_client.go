package orders

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	queuePublish          = "/v1/queue/publish"
	queueConsume          = "/v1/queue/consume"
	queueAck              = "/v1/queue/ack"
	ordersQueue           = "ecommerce-orders"
	ordersDeadLetterQueue = "ecommerce-orders-dead-letter"
)

// APIError preserves an Infrai business rejection for the service boundary.
type APIError struct {
	Status int
	Code   string
	Detail any
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return "infrai: " + e.Code
	}
	return "infrai: request rejected"
}

type envelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    json.RawMessage `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

type errorBody struct {
	Code string `json:"code"`
}

type Message struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

type consumeData struct {
	Messages []Message `json:"messages"`
}

type Client struct {
	baseURL    string
	key        string
	httpClient *http.Client
	sleep      func(context.Context, time.Duration) error
	maxRetries int
}

func NewClient(baseURL, key string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 20 * time.Second}
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		key:        key,
		httpClient: httpClient,
		maxRetries: 3,
		sleep: func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
}

func (c *Client) Consume(ctx context.Context, maxMessages, visibilityTimeout int) ([]Message, error) {
	var data consumeData
	err := c.call(ctx, http.MethodPost, queueConsume, map[string]any{
		"queue":              ordersQueue,
		"max_messages":       maxMessages,
		"visibility_timeout": visibilityTimeout,
	}, "", &data)
	return data.Messages, err
}

// QueuePublish is the copyable publish idiom; the key makes retries apply once.
func (c *Client) QueuePublish(ctx context.Context, payload any, idempotencyKey string) error {
	return c.call(ctx, http.MethodPost, queuePublish, map[string]any{
		"queue":   ordersDeadLetterQueue,
		"payload": payload,
	}, idempotencyKey, nil)
}

func (c *Client) Ack(ctx context.Context, messageID string) error {
	return c.call(ctx, http.MethodPost, queueAck, map[string]string{
		"queue":      ordersQueue,
		"message_id": messageID,
	}, "", nil)
}

func (c *Client) call(ctx context.Context, method, path string, body any, idempotencyKey string, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(encoded))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+c.key)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("send request: %w", err)
		}
		raw, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}

		var env envelope
		decodeErr := json.Unmarshal(raw, &env)
		if decodeErr == nil && !env.OK {
			var detail errorBody
			_ = json.Unmarshal(env.Error, &detail)
			if res.StatusCode == http.StatusTooManyRequests && attempt < c.maxRetries {
				if err := c.sleep(ctx, retryDelay(res.Header.Get("Retry-After"), attempt)); err != nil {
					return err
				}
				continue
			}
			return &APIError{Status: res.StatusCode, Code: detail.Code, Detail: json.RawMessage(env.Error)}
		}
		if decodeErr != nil {
			return fmt.Errorf("decode envelope: %w", decodeErr)
		}
		if res.StatusCode >= http.StatusInternalServerError {
			return fmt.Errorf("infrai transport status: %d", res.StatusCode)
		}
		if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return fmt.Errorf("decode data: %w", err)
			}
		}
		return nil
	}
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second * time.Duration(1<<attempt)
}

func IsBusinessRejection(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status >= 400 && apiErr.Status < 500
}
