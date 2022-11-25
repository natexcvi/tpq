package main

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func Setup() error {
	cmd := exec.Command("docker", "compose", "up", "--wait", "--remove-orphans")
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to run docker compose up: %w", err)
	}
	return nil
}

func Cleanup() error {
	cmd := exec.Command("docker", "compose", "down")
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to run docker compose down: %w", err)
	}
	return nil
}

func TestSetupCleanup(t *testing.T) {
	Setup()
	defer Cleanup()
}
