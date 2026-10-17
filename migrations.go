package durable

import (
	"context"
	"crypto/sha256"
	"embed"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const migrationLock = 730041221

type migration struct{ name, sql, checksum string }

func migrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, err
	}
	var result []migration
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		raw, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, err
		}
		sql := strings.ReplaceAll(string(raw), "\r\n", "\n")
		result = append(result, migration{entry.Name(), sql, fmt.Sprintf("%x", sha256.Sum256([]byte(sql)))})
	}
	return result, nil
}

// migrationPrefix accepts only an ordered prefix of this binary's immutable
// files. NULL checksums belong to the legacy catalog and are adopted by Migrate.
func migrationPrefix(ctx context.Context, tx pgx.Tx, known []migration, requireCurrent bool) (int, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		if requireCurrent {
			return 0, fmt.Errorf("%w: run migrations before execution", ErrIncompatibleSchema)
		}
		return 0, nil
	}
	rows, err := tx.Query(ctx, `SELECT name,to_jsonb(m)->>'checksum' FROM schema_migrations m ORDER BY name`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name string
		var checksum *string
		if err := rows.Scan(&name, &checksum); err != nil {
			return 0, err
		}
		if count >= len(known) || name != known[count].name {
			return 0, fmt.Errorf("%w: unknown or out-of-order migration %q", ErrIncompatibleSchema, name)
		}
		if checksum != nil && *checksum != known[count].checksum {
			return 0, fmt.Errorf("%w: migration %q checksum differs", ErrIncompatibleSchema, name)
		}
		if requireCurrent && checksum == nil {
			return 0, fmt.Errorf("%w: run migrations to adopt legacy checksums", ErrIncompatibleSchema)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if requireCurrent && count != len(known) {
		return 0, fmt.Errorf("%w: run pending migrations", ErrIncompatibleSchema)
	}
	return count, nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	known, err := migrations()
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer rollbackSchemaTx(tx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLock); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY,applied_at timestamptz NOT NULL,checksum text); ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum text`); err != nil {
		return err
	}
	prefix, err := migrationPrefix(ctx, tx, known, false)
	if err != nil {
		return err
	}
	for i, m := range known {
		if i < prefix {
			if _, err := tx.Exec(ctx, `UPDATE schema_migrations SET checksum=$2 WHERE name=$1 AND checksum IS NULL`, m.name, m.checksum); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("apply migration %q: %w", m.name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(name,applied_at,checksum) VALUES($1,clock_timestamp(),$2)`, m.name, m.checksum); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// CheckSchema rejects missing, altered, and unknown migrations. Opening an engine
// allows a known older schema so its owner can call Migrate first.
func (e *Engine) CheckSchema(ctx context.Context) error {
	tx, err := e.beginExecution(ctx)
	if err != nil {
		return err
	}
	return tx.Rollback(ctx)
}

func (e *Engine) beginExecution(ctx context.Context) (pgx.Tx, error) {
	started := time.Now()
	tx, err := checkSchemaTransaction(ctx, e.pool, true)
	if err != nil {
		return nil, err
	}
	if len(e.observers) > 0 {
		return &observedTx{Tx: tx, engine: e, started: started}, nil
	}
	return tx, nil
}

func checkSchemaTransaction(ctx context.Context, pool *pgxpool.Pool, current bool) (pgx.Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	// Shared until commit: migration DDL cannot overtake a checked mutation.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, migrationLock); err == nil {
		var known []migration
		known, err = migrations()
		if err == nil {
			_, err = migrationPrefix(ctx, tx, known, current)
		}
	}
	if err != nil {
		rollbackSchemaTx(tx)
		return nil, err
	}
	return tx, nil
}

func rollbackSchemaTx(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
