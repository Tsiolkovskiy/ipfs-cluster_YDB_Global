package main

import (
	"context"
	"fmt"
	"log"

	"github.com/global-data-controller/gdc/internal/config"
)

func main() {
	fmt.Println("Seeding GDC database with test data...")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	ctx := context.Background()

	// TODO: Implement test data seeding
	// This will be implemented in task 4 when we create the MetadataStore
	fmt.Printf("Database endpoint: %s\n", cfg.Database.Endpoint)
	fmt.Printf("Database name: %s\n", cfg.Database.Database)

	// Test data will include:
	// - Sample zones (MSK, NN)
	// - Sample clusters
	// - Sample policies
	// - Sample content and replicas

	fmt.Println("Database seeding completed (placeholder)")
	_ = ctx
}