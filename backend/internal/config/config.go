package config

import (
	"os"
	"strconv"
)

type Config struct {
	MySQLDSN       string
	HTTPAddr       string
	FrontendOrigin string
	MySQLDatabase  string
	BinlogEnabled  bool
	BinlogHost     string
	BinlogPort     uint16
	BinlogUser     string
	BinlogPassword string
	BinlogServerID uint32
}

func Load() Config {
	return Config{
		MySQLDSN:       env("MYSQL_DSN", "cvpr:cvpr_pass@tcp(127.0.0.1:3306)/cvpr_display?charset=utf8mb4&parseTime=true&loc=Local"),
		HTTPAddr:       env("HTTP_ADDR", ":8080"),
		FrontendOrigin: env("FRONTEND_ORIGIN", "http://localhost:5173"),
		MySQLDatabase:  env("MYSQL_DATABASE", "cvpr_display"),
		BinlogEnabled:  envBool("BINLOG_ENABLED", true),
		BinlogHost:     env("BINLOG_HOST", "127.0.0.1"),
		BinlogPort:     uint16(envInt("BINLOG_PORT", 3306)),
		BinlogUser:     env("BINLOG_USER", "replicator"),
		BinlogPassword: env("BINLOG_PASSWORD", "replicator_pass"),
		BinlogServerID: uint32(envInt("BINLOG_SERVER_ID", 2001)),
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
