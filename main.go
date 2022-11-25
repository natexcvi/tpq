package main

import (
	"context"

	"github.com/natexcvi/tpq/orchestrator"
)

func main() {
	ctx := context.Background()
	config := orchestrator.NewConfigFromEnv()
	srv := orchestrator.NewTaskOrchestratorServer(config)
	srv.StartServer(ctx)
}
