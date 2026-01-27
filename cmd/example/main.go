package main

import (
	"context"
	"net"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/blueai2022/go_streaming/internal/config"
	"github.com/rs/zerolog/log"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	settings, err := config.New()
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("failed to create settings")

		return
	}

	log.Debug().
		Any("settings", settings).
		Msg("loaded configuration")

	mux := http.NewServeMux()

	httpServer := &http.Server{
		Addr:    settings.HTTP.Address(),
		Handler: mux,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}

	go func() {
		log.Info().
			Str("eventAction", "start").
			Msg("starting http example server")

		if err := httpServer.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				log.Info().
					Str("eventAction", "stop").
					Msg("http server closed")

				return
			}

			log.Fatal().
				Err(err).
				Msg("http server exited with error")
		}
	}()

	defer cancel()

	<-ctx.Done()

	log.Warn().
		Str("eventAction", "shutdown").
		Msg("shutting down http server")

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		settings.HTTP.ShutdownTimeout,
	)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error().
			Err(err).
			Msg("http server shutdown error")
	}

	log.Warn().
		Str("eventAction", "shutdown").
		Msg("shutting down esl outbound server")
}
