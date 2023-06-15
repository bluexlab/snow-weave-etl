package snow

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type SnowflakeSqlExecutor struct {
	db             *sql.DB
	parallelDegree int
}

type SnowflakeSqlExecutorOption func(*SnowflakeSqlExecutor)

func WithDb(db *sql.DB) SnowflakeSqlExecutorOption {
	return func(s *SnowflakeSqlExecutor) {
		s.db = db
	}
}

func WithParallelDegree(degree int) SnowflakeSqlExecutorOption {
	return func(s *SnowflakeSqlExecutor) {
		s.parallelDegree = degree
	}
}

func NewSnowflakeSqlExecutor(options ...SnowflakeSqlExecutorOption) *SnowflakeSqlExecutor {
	s := &SnowflakeSqlExecutor{
		parallelDegree: 5,
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

func (s *SnowflakeSqlExecutor) Execute(ctx context.Context, snowSqls []SnowflakeSql) error {
	g, ctx := errgroup.WithContext(ctx)

	snowSqlCh := make(chan SnowflakeSql, len(snowSqls))
	for _, snowSql := range snowSqls {
		snowSqlCh <- snowSql
	}

	close(snowSqlCh)

	for i := 0; i < s.parallelDegree; i++ {
		g.Go(func() error {
			for snowSql := range snowSqlCh {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				logrus.Infof("SnowflakeSqlExecutor.Execute() executing sql %q", snowSql.FileName)

				if err := s.executeSql(ctx, snowSql); err != nil {
					return fmt.Errorf("fail to execute sql %q. %w", snowSql.FileName, err)
				}
			}

			return nil
		})
	}

	return g.Wait()
}

func (s *SnowflakeSqlExecutor) executeSql(ctx context.Context, snowSql SnowflakeSql) error {
	logrus.Debugf("SnowflakeSqlExecutor.executeSql %s", snowSql.FileName)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return err
	}

	defer func(tx *sql.Tx) { _ = tx.Rollback() }(tx)

	_, execErr := tx.ExecContext(ctx, snowSql.Sql)
	if execErr != nil {
		return execErr
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
