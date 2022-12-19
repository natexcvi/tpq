package logger

import (
	"github.com/rs/zerolog"
)

func InitLogger() {
	// zerolog.TimeFieldFormat = zerolog.TimeFormatUnix - For faster logging, non human readable
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}
