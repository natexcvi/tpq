package main

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/natexcvi/tpq/logger"
	"github.com/natexcvi/tpq/orchestrator"
)

func init() {
	logger.InitLogger()
}

func main() {
	log.Info().Msg("Starting to run program")
	ctx := context.Background()
	config := orchestrator.NewConfigFromEnv()
	srv := orchestrator.NewTaskOrchestratorServer(config)
	srv.StartServer(ctx)
}
