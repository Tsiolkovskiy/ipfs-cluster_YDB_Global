package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

func main() {
	// Test viper environment variable handling
	v := viper.New()
	v.SetEnvPrefix("GDC")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	
	// Set environment variable
	os.Setenv("GDC_TELEMETRY_JAEGER_ENDPOINT", "http://test:14268")
	
	// Try to get the value
	endpoint := v.GetString("telemetry.jaeger_endpoint")
	fmt.Printf("Endpoint from viper: '%s'\n", endpoint)
	
	// Check if env var is set
	envVal := os.Getenv("GDC_TELEMETRY_JAEGER_ENDPOINT")
	fmt.Printf("Env var value: '%s'\n", envVal)
}