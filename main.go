package main

import (
	"context"
	"database/sql"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/alecthomas/kingpin/v2"
	formatter "github.com/bluexlab/logrus-formatter"
	"github.com/bluexlab/snow-weave-etl/pkg/logger"
	"github.com/bluexlab/snow-weave-etl/pkg/snow"
	"github.com/sirupsen/logrus"
	"github.com/snowflakedb/gosnowflake"
)

const (
	appName = "snow-weave-etl"
	appDesc = "Snow Weave ETL executes a bunch of SQL queries to perform ETL in Snowflake."
)

func newSnowLogger() gosnowflake.SFLogger {
	format := &formatter.Formatter{
		HideKeys:       true,
		ShowFullLevel:  true,
		NoColors:       true,
		NoFieldsColors: true,
	}
	snowLogger := logrus.New()
	snowLogger.SetFormatter(format)
	snowLogger.SetOutput(os.Stdout)
	snowLogger.SetLevel(logrus.InfoLevel)

	return logger.NewSnowLogger(snowLogger)
}

func main() {
	app := kingpin.New(appName, appDesc)
	app.Version("1.0.0")
	app.HelpFlag.Short('h')

	etlSourceFolder := app.Flag("path", "Path to the ETL SQL folder").Short('p').Default("sql").String()

	_ = kingpin.MustParse(app.Parse(os.Args[1:]))

	formatter.InitLogger()
	// Set snowflake logger
	sfLog := newSnowLogger()
	gosnowflake.SetLogger(&sfLog)

	// Create context that listens for the interrupt signal from the OS.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runETL(ctx, *etlSourceFolder)

	logrus.Info("Done.")
}

func runETL(ctx context.Context, etlSourceFolder string) {
	// Prepare database
	db, err := prepareDb()
	if err != nil {
		logrus.Fatalf("Fail to prepare database. %v", err)
	}

	defer func(c io.Closer) { _ = c.Close() }(db)

	logrus.Info("Loading SQL files")

	sqlFiles, err := snow.LoadSqls(etlSourceFolder)
	if err != nil {
		logrus.Fatalf("Fail to load SQL files from %q. %v", etlSourceFolder, err)
	}

	var sqlParallelDegree int = 5

	if v := os.Getenv("SQL_PARALLEL_DEGREE"); len(v) > 0 {
		if i, _ := strconv.ParseInt(v, 10, 64); i > 0 {
			sqlParallelDegree = int(i)
		}
	}

	sqlExecutor := snow.NewSnowflakeSqlExecutor(
		snow.WithDb(db),
		snow.WithParallelDegree(sqlParallelDegree),
	)

	logrus.Info("runETL() start to execute SQL files")

	if err := sqlExecutor.Execute(ctx, sqlFiles); err != nil {
		logrus.Errorf("Fail to execute SQL files. %v", err)
	}

	logrus.Info("runETL() finished executing SQL files")
}

func prepareDb() (*sql.DB, error) {
	config := &gosnowflake.Config{
		Account:   os.Getenv("SNOWFLAKE_ACCOUNT"),
		User:      os.Getenv("SNOWFLAKE_USERNAME"),
		Password:  os.Getenv("SNOWFLAKE_PASSWORD"),
		Database:  os.Getenv("SNOWFLAKE_DATABASE"),
		Schema:    os.Getenv("SNOWFLAKE_SCHEMA"),
		Warehouse: os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Role:      os.Getenv("SNOWFLAKE_ROLE"),
	}
	dsn, err := gosnowflake.DSN(config)

	if err != nil {
		return nil, err
	}

	logrus.Debug("Opening Connection to Snowflake ...")

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}

	logrus.Debug("Connecting to Snowflake ...")

	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
