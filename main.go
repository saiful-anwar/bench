package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

var wg sync.WaitGroup
var pool *pgxpool.Pool
var limit int
var batchSize int

type Result struct {
	Type    string
	Err     error
	Message string
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	dataLimit := os.Getenv("DATA_LIMIT")
	limit, err = strconv.Atoi(dataLimit)
	if err != nil {
		fmt.Println("Error converting string to int:", err)
		return
	}

	dataBatch := os.Getenv("DATA_BATCH_SIZE")
	batchSize, err = strconv.Atoi(dataBatch)
	if err != nil {
		fmt.Println("Error converting string to int:", err)
		return
	}

	host := os.Getenv("DB_HOST")
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASS")
	port := os.Getenv("DB_PORT")
	db := os.Getenv("DB_NAME")

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, db)

	// Create a connection pool
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("Unable to parse DSN: %v", err)
	}

	ctx := context.Background()
	pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}

	defer pool.Close()
	errorChan := make(chan Result, 1)

	wg.Add(3)
	go fetchWithCursor(ctx, errorChan)
	go fetchWithOffsetLimit(ctx, errorChan)
	go fetchWithCustomCursor(ctx, errorChan)

	go func() {
		wg.Wait()
		close(errorChan)
	}()

	for result := range errorChan {
		if result.Err != nil {
			fmt.Println(result.Err)
		} else {
			fmt.Println(result.Message)
		}
	}
}

func fetchWithCursor(ctx context.Context, res chan<- Result) error {
	defer wg.Done()
	start := time.Now()
	result := Result{
		Type: "cursor",
	}

	// Create or open the CSV file
	file, err := os.Create("./output/cursor.csv")
	if err != nil {
		err = fmt.Errorf("error creating file: %v", err)
		result.Err = err
		res <- result
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush() // Ensure data is written to file

	// Start a transaction
	tx, err := pool.Begin(ctx)
	if err != nil {
		err = fmt.Errorf("failed to begin transaction: %w", err)
		result.Err = err
		res <- result
		return err
	}
	defer tx.Rollback(ctx)

	// Declare a cursor for a large query
	cursorQuery := fmt.Sprintf(`
        DECLARE my_cursor CURSOR FOR 
        SELECT aid, bid, abalance
        FROM pgbench_accounts
		WHERE aid <= %d
		ORDER BY aid ASC`, limit)

	_, err = tx.Exec(ctx, cursorQuery)
	if err != nil {
		err = fmt.Errorf("failed to declare cursor: %w", err)
		result.Err = err
		res <- result
		return err
	}

	for {
		// Fetch the next batch of rows
		fetchQuery := fmt.Sprintf("FETCH %d FROM my_cursor", batchSize)
		rows, err := tx.Query(ctx, fetchQuery)
		if err != nil {
			err = fmt.Errorf("failed to fetch data: %w", err)
			result.Err = err
			res <- result
			return err
		}

		// Check if there are no more rows
		if !rows.Next() {
			rows.Close()
			break
		}

		// Process each row in the batch
		for rows.Next() {
			var aid, bid, abalance int
			// var name, email string

			if err := rows.Scan(&aid, &bid, &abalance); err != nil {
				err = fmt.Errorf("failed to scan row: %w", err)
				result.Err = err
				res <- result
				return err
			}

			record := []string{
				fmt.Sprintf("%d", aid),
				fmt.Sprintf("%d", bid),
				fmt.Sprintf("%d", abalance),
			}

			if err := writer.Write(record); err != nil {
				err = fmt.Errorf("error writing record to CSV: %v", err)
				result.Err = err
				res <- result
				return err
			}
		}

		rows.Close()

		if rows.Err() != nil {
			err = fmt.Errorf("error occurred while iterating rows: %w", rows.Err())
			result.Err = err
			res <- result
			return err
		}
	}

	// Close the cursor explicitly
	_, err = tx.Exec(ctx, "CLOSE my_cursor")
	if err != nil {
		err = fmt.Errorf("failed to close cursor: %w", err)
		result.Err = err
		res <- result
		return err
	}

	// Commit the transaction
	commit := tx.Commit(ctx)

	// Record the end time
	end := time.Now()
	duration := end.Sub(start)

	result.Message = fmt.Sprintf("cursor done in %.2f second", duration.Seconds())
	res <- result

	return commit
}

func fetchWithCustomCursor(ctx context.Context, res chan<- Result) error {
	defer wg.Done()
	start := time.Now()
	result := Result{
		Type: "custom_cursor",
	}

	// Create or open the CSV file
	file, err := os.Create("./output/custom_cursor.csv")
	if err != nil {
		err = fmt.Errorf("error creating file: %v", err)
		result.Err = err
		res <- result
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush() // Ensure data is written to file

	var lastId int
	for {
		// Construct the query with limit and offset
		query := fmt.Sprintf(`
			SELECT aid, bid, abalance
			FROM pgbench_accounts
			WHERE aid > %d AND aid <= %d
			ORDER BY aid ASC
			LIMIT %d`, lastId, limit, batchSize)

		// Execute the query
		rows, err := pool.Query(ctx, query)
		if err != nil {
			err = fmt.Errorf("failed to fetch data: %w", err)
			result.Err = err
			res <- result
			return err
		}

		// Check if there are no more rows
		if !rows.Next() {
			rows.Close()
			break
		}

		// Process each row in the batch
		for rows.Next() {
			var aid, bid, abalance int

			if err := rows.Scan(&aid, &bid, &abalance); err != nil {
				err = fmt.Errorf("failed to scan row: %w", err)
				result.Err = err
				res <- result
				return err
			}

			// Prepare the record for CSV
			record := []string{
				fmt.Sprintf("%d", aid),
				fmt.Sprintf("%d", bid),
				fmt.Sprintf("%d", abalance),
			}

			if err := writer.Write(record); err != nil {
				err = fmt.Errorf("error writing record to CSV: %v\n", err)
				result.Err = err
				res <- result
				return err
			}

			lastId = aid
		}

		rows.Close()

		if rows.Err() != nil {
			err = fmt.Errorf("error occurred while iterating rows: %w", rows.Err())
			result.Err = err
			res <- result
			return err
		}
	}

	end := time.Now()
	duration := end.Sub(start)

	result.Message = fmt.Sprintf("custom cursor done in %.2f second", duration.Seconds())
	res <- result

	return nil
}

func fetchWithOffsetLimit(ctx context.Context, res chan<- Result) error {
	defer wg.Done()
	start := time.Now()
	result := Result{
		Type: "offset_limit",
	}

	// Create or open the CSV file
	file, err := os.Create("./output/offset_limit.csv")
	if err != nil {
		err = fmt.Errorf("error creating file: %v", err)
		result.Err = err
		res <- result
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush() // Ensure data is written to file

	// Set the batch size and initialize the offset
	offset := 0

	for {
		// Construct the query with limit and offset
		query := fmt.Sprintf(`
			SELECT aid, bid, abalance
			FROM pgbench_accounts
			WHERE aid <= %d
			ORDER BY aid ASC
			OFFSET %d LIMIT %d`, limit, offset, batchSize)

		// Execute the query
		rows, err := pool.Query(ctx, query)
		if err != nil {
			err = fmt.Errorf("failed to fetch data: %w", err)
			result.Err = err
			res <- result
			return err
		}

		// Check if there are no more rows
		if !rows.Next() {
			rows.Close()
			break
		}

		// Process each row in the batch
		for rows.Next() {
			var aid, bid, abalance int

			if err := rows.Scan(&aid, &bid, &abalance); err != nil {
				err = fmt.Errorf("failed to scan row: %w", err)
				result.Err = err
				res <- result
				return err
			}

			// Prepare the record for CSV
			record := []string{
				fmt.Sprintf("%d", aid),
				fmt.Sprintf("%d", bid),
				fmt.Sprintf("%d", abalance),
			}

			if err := writer.Write(record); err != nil {
				fmt.Errorf("error writing record to CSV: %v\n", err)
				result.Err = err
				res <- result
				return err
			}
		}

		rows.Close()

		if rows.Err() != nil {
			err = fmt.Errorf("error occurred while iterating rows: %w", rows.Err())
			result.Err = err
			res <- result
			return err
		}

		// Update the offset for the next batch
		offset += batchSize
	}

	end := time.Now()
	duration := end.Sub(start)
	result.Message = fmt.Sprintf("offset limit done in %.2f second", duration.Seconds())
	res <- result

	return nil
}
