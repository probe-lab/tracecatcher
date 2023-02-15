package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

var options struct {
	addr                 string
	verbose              bool
	veryverbose          bool
	diagnosticsAddr      string
	dbHost               string
	dbPort               int
	dbName               string
	dbUser               string
	dbPassword           string
	dbSSLMode            string
	batchSize            int
	metricReportInterval int
}

var envPrefix = "TRACECATCHER_"

var app = &cli.App{
	Name:     "tracecatcher",
	HelpName: "tracecatcher",
	Usage:    "Listens to gossipsub traces emitted from Lotus and stores them in postgresql.",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "addr",
			Aliases:     []string{"a"},
			Usage:       "Listen for traces on `ADDRESS:PORT`",
			Value:       ":5151",
			EnvVars:     []string{envPrefix + "ADDR"},
			Destination: &options.addr,
		},
		&cli.StringFlag{
			Name:        "diag-addr",
			Aliases:     []string{"da"},
			Usage:       "Run diagnostics server for metrics on `ADDRESS:PORT`",
			Value:       "",
			EnvVars:     []string{envPrefix + "DIAG_ADDR"},
			Destination: &options.diagnosticsAddr,
		},
		&cli.BoolFlag{
			Name:        "verbose",
			Aliases:     []string{"v"},
			Usage:       "Set logging level more verbose to include info level logs",
			Value:       false,
			EnvVars:     []string{envPrefix + "VERBOSE"},
			Destination: &options.verbose,
		},
		&cli.BoolFlag{
			Name:        "veryverbose",
			Aliases:     []string{"vv"},
			Usage:       "Set logging level very verbose to include debug level logs",
			Value:       false,
			EnvVars:     []string{envPrefix + "VERY_VERBOSE"},
			Destination: &options.veryverbose,
		},
		&cli.StringFlag{
			Name:        "db-host",
			Usage:       "The hostname/address of the database server",
			EnvVars:     []string{envPrefix + "DB_HOST"},
			Destination: &options.dbHost,
		},
		&cli.IntFlag{
			Name:        "db-port",
			Usage:       "The port number of the database server",
			EnvVars:     []string{envPrefix + "DB_PORT"},
			Value:       5432,
			Destination: &options.dbPort,
		},
		&cli.StringFlag{
			Name:        "db-name",
			Usage:       "The name of the database to use",
			EnvVars:     []string{envPrefix + "DB_NAME"},
			Destination: &options.dbName,
		},
		&cli.StringFlag{
			Name:        "db-password",
			Usage:       "The password to use when connecting the the database",
			EnvVars:     []string{envPrefix + "DB_PASSWORD"},
			Destination: &options.dbPassword,
		},
		&cli.StringFlag{
			Name:        "db-user",
			Usage:       "The user to use when connecting the the database",
			EnvVars:     []string{envPrefix + "DB_USER"},
			Destination: &options.dbUser,
		},
		&cli.StringFlag{
			Name:        "db-sslmode",
			Usage:       "The sslmode to use when connecting the the database",
			EnvVars:     []string{envPrefix + "DB_SSL_MODE"},
			Value:       "prefer",
			Destination: &options.dbSSLMode,
		},
		&cli.IntFlag{
			Name:        "batch-size",
			Aliases:     []string{"b"},
			Usage:       "The size of query batches to use when inserting into the database",
			EnvVars:     []string{envPrefix + "BATCH_SIZE"},
			Value:       100,
			Destination: &options.batchSize,
		},
		&cli.IntFlag{
			Name:        "metric-report-interval",
			Usage:       "The interval (in seconds) on which metrics should be updated",
			EnvVars:     []string{envPrefix + "METRIC_REPORT_INTERVAL"},
			Value:       30,
			Destination: &options.metricReportInterval,
		},
	},
	Action:          run,
	HideHelpCommand: true,
}

func Run(args []string) error {
	return app.Run(os.Args)
}

func run(cc *cli.Context) error {
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelWarn)
	slog.SetDefault(slog.New(slog.HandlerOptions{Level: logLevel}.NewTextHandler(os.Stdout)))

	if options.verbose {
		logLevel.Set(slog.LevelInfo)
	}
	if options.veryverbose {
		logLevel.Set(slog.LevelDebug)
	}

	ctx, cancel := context.WithCancel(cc.Context)
	defer cancel()

	conn, err := connect(ctx, options.dbHost, options.dbPort, options.dbName, options.dbSSLMode, options.dbUser, options.dbPassword)
	if err != nil {
		slog.Error("pgconn failed to connect", err)
		return err
	}
	defer func() {
		slog.Info("closing database connection")
		conn.Close(context.Background())
	}()

	rg := new(RunGroup)

	// Init metric reporting if required
	if options.diagnosticsAddr != "" {
		if err := InitMetricReporting(time.Duration(options.metricReportInterval) * time.Second); err != nil {
			return fmt.Errorf("failed to initialize metric reporting: %w", err)
		}
		dr := &DiagRunner{
			addr: options.diagnosticsAddr,
		}
		rg.Add(dr)
	}

	bat, err := NewBatcher(conn, options.batchSize)
	if err != nil {
		return fmt.Errorf("failed to create batcher: %w", err)
	}

	svr, err := NewServer(bat)
	if err != nil {
		return fmt.Errorf("failed to create web server: %w", err)
	}

	p := &WebRunner{
		server: svr,
	}
	rg.Add(p)

	return rg.RunAndWait(ctx)
}

// Runnable allows a component to be started.
type Runnable interface {
	// Run starts running the component and blocks until the context is canceled, Shutdown is // called or a fatal error is encountered.
	Run(context.Context) error
}

type RunGroup struct {
	runnables []Runnable
}

func (rg *RunGroup) Add(r Runnable) {
	rg.runnables = append(rg.runnables, r)
}

func (rg *RunGroup) RunAndWait(ctx context.Context) error {
	if len(rg.runnables) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	for i := range rg.runnables {
		r := rg.runnables[i]
		g.Go(func() error { return r.Run(ctx) })
	}

	// Ensure components stop if we receive a terminating operating system signal.
	g.Go(func() error {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)
		select {
		case <-interrupt:
			cancel()
		case <-ctx.Done():
		}
		return nil
	})

	// Wait for all servers to run to completion.
	if err := g.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}

type WebRunner struct {
	server *Server
}

func (r *WebRunner) Run(ctx context.Context) error {
	mx := mux.NewRouter()

	r.server.ConfigureRoutes(mx)

	srv := &http.Server{
		Handler:     mx,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	go func() {
		<-ctx.Done()
		if err := srv.Shutdown(context.Background()); err != nil {
			slog.Error("failed to shut down RPC server", err)
		}
	}()

	slog.Info("starting server", "addr", options.addr)
	listener, err := net.Listen("tcp", options.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %q: %w", options.addr, err)
	}

	if err := srv.Serve(listener); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve failed: %w", err)
		}
	}

	return nil
}

type DiagRunner struct {
	addr string
}

func (dr *DiagRunner) Run(ctx context.Context) error {
	diagListener, err := net.Listen("tcp", dr.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %q: %w", dr.addr, err)
	}

	pe, err := RegisterPrometheusExporter("tracecatcher")
	if err != nil {
		return fmt.Errorf("failed to register prometheus exporter: %w", err)
	}

	mx := mux.NewRouter()
	mx.Handle("/metrics", pe)

	srv := &http.Server{
		Handler:     mx,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	go func() {
		<-ctx.Done()
		if err := srv.Shutdown(context.Background()); err != nil {
			slog.Error("failed to shut down diagnostics server", err)
		}
	}()

	slog.Info("starting diagnostics server", "addr", dr.addr)
	return srv.Serve(diagListener)
}
