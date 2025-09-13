package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run setup-ydb-schema.go <connection_string>")
	}

	connectionString := os.Args[1]
	ctx := context.Background()

	// Connect to YDB
	driver, err := ydb.Open(ctx, connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to YDB: %v", err)
	}
	defer driver.Close(ctx)

	// Read schema file
	schemaContent, err := ioutil.ReadFile("internal/storage/schema.sql")
	if err != nil {
		log.Fatalf("Failed to read schema file: %v", err)
	}

	// Split schema into individual statements
	statements := strings.Split(string(schemaContent), ";")

	// Execute each DDL statement
	err = driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		for _, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" || strings.HasPrefix(stmt, "--") {
				continue
			}

			fmt.Printf("Executing: %s\n", stmt)
			_, _, err := s.Execute(ctx, table.DefaultTxControl(), stmt, nil)
			if err != nil {
				return fmt.Errorf("failed to execute statement '%s': %w", stmt, err)
			}
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Failed to setup schema: %v", err)
	}

	fmt.Println("YDB schema setup completed successfully!")
}