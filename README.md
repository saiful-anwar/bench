# Proof of Concept: Fetch Data from PostgreSQL and Save to CSV

This project demonstrates different methods of fetching data from a PostgreSQL database and save the output to a CSV file. The performance of each method can be compared to determine the most efficient approach.

## Overview

The application uses three different methods to fetch data from PostgreSQL:

1. **Native PostgreSQL Cursor**: Uses PostgreSQL’s native cursor for fetching data in batches to reduce memory usage.
2. **Custom Cursor**: Implements a custom cursor logic to fetch data in chunks.
3. **Offset-Limit Pagination**: Fetches data using the `OFFSET` and `LIMIT` SQL keywords.

## Prerequisites

- **Go** installed (version 1.16 or higher is recommended)
- **PostgreSQL** installed and running
- **pgbench** utility to generate sample data

## Generate Sample Data

To simulate a large dataset, use `pgbench` to generate approximately 1 billion rows in the PostgreSQL database:

```bash
pgbench -i -s 10 pgbench_db
```

The -s 150 option sets the scaling factor, generating around 1 billion rows. Adjust the scaling factor as needed based on available resources.

## Configuration
1. Copy the example environment file and rename it:
```
cp env.example .env
```
2. Update the .env file with your PostgreSQL database credentials:
```
DB_HOST=your_host
DB_USER=your_user
DB_PASS=your_password
DB_PORT=your_port
DB_NAME=pgbench_db
```

## How to Run the Application
Run the main program with the following command:
```
go run main.go
```
This will execute the data-fetching process using the specified methods and save the results into a CSV file.

## Sample Test Result
```
➜ go run main.go
cursor done in 2.36 second
custom cursor done in 5.27 second
offset limit done in 548.01 second
```