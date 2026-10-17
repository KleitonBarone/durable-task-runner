package durable

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// PageRequest bounds a live scan. Sensitive exposes diagnostic text and history
// details, never lease tokens. Application result APIs remain the payload path.
type PageRequest struct {
	Limit     int
	Cursor    string
	Sensitive bool
}

type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type pageCursor struct {
	Scope string `json:"scope"`
	After int64  `json:"after"`
	Upper int64  `json:"upper"`
}

func parsePage(request PageRequest, scope any) (PageRequest, pageCursor, error) {
	if request.Limit == 0 {
		request.Limit = 50
	}
	if request.Limit < 1 || request.Limit > 500 || len(request.Cursor) > 4096 {
		return request, pageCursor{}, fmt.Errorf("%w: page limit must be 1-500", ErrInvalidInput)
	}
	encoded, err := json.Marshal(struct {
		Scope     any
		Sensitive bool
	}{scope, request.Sensitive})
	if err != nil {
		return request, pageCursor{}, err
	}
	want := fmt.Sprintf("%x", sha256.Sum256(encoded))
	cursor := pageCursor{Scope: want}
	if request.Cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(request.Cursor)
		if err != nil {
			return request, cursor, fmt.Errorf("%w: invalid page cursor", ErrInvalidInput)
		}
		if err := json.Unmarshal(raw, &cursor); err != nil || cursor.Scope != want || cursor.After < 0 || cursor.Upper < cursor.After {
			return request, cursor, fmt.Errorf("%w: cursor does not match query", ErrInvalidInput)
		}
	}
	return request, cursor, nil
}

// readPage uses only internal SQL/key identifiers. The cursor is query-bound,
// not an authorization token; database access remains application-owned.
func readPage[T any](ctx context.Context, e *Engine, request PageRequest, scope any, query, key string, args []any, scan func(pgx.Rows, time.Time) (T, int64, error)) (Page[T], error) {
	request, cursor, err := parsePage(request, scope)
	if err != nil {
		return Page[T]{}, err
	}
	tx, err := e.beginExecution(ctx)
	if err != nil {
		return Page[T]{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now, err := e.now(ctx, tx)
	if err != nil {
		return Page[T]{}, err
	}
	if request.Cursor == "" {
		if err := tx.QueryRow(ctx, "SELECT COALESCE(max(page_key),0) FROM ("+query+") page", args...).Scan(&cursor.Upper); err != nil {
			return Page[T]{}, err
		}
	}
	n := len(args)
	query += fmt.Sprintf(" AND %s > $%d AND %s <= $%d ORDER BY %s LIMIT $%d", key, n+1, key, n+2, key, n+3)
	args = append(args, cursor.After, cursor.Upper, request.Limit+1)
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return Page[T]{}, err
	}
	defer rows.Close()
	page := Page[T]{Items: make([]T, 0, request.Limit)}
	var last int64
	for rows.Next() {
		item, key, err := scan(rows, now)
		if err != nil {
			return Page[T]{}, err
		}
		if len(page.Items) == request.Limit {
			cursor.After = last
			raw, err := json.Marshal(cursor)
			if err != nil {
				return Page[T]{}, err
			}
			page.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
			break
		}
		last = key
		page.Items = append(page.Items, item)
	}
	rows.Close()
	return page, rows.Err()
}
