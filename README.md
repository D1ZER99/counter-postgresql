# PostgreSQL Counter Implementation - Part 01

This folder contains the PostgreSQL database setup for implementing a like/view/retweet counter with race condition protection and data persistence.

## Project Structure

```
postgres-insert/
├── docker-compose.yml    # Docker configuration for PostgreSQL
├── init.sql             # Database initialization script
├── .env                 # Environment variables (database credentials)
├── postgres_data/       # Persistent data directory (auto-created)
└── README.md           # This file
```

## Database Schema

### Table: `user_counter`

| Column   | Type    | Description                                      |
|----------|---------|--------------------------------------------------|
| user_id  | INTEGER | Primary key - unique user identifier             |
| counter  | INTEGER | Counter value (likes/views/retweets)            |
| version  | INTEGER | Auxiliary field for optimistic locking          |

## Setup Instructions

### Prerequisites

- Docker Desktop installed and running
- Docker Compose installed

### Starting the Database

1. Open a terminal in the `postgres-insert` directory

2. Start PostgreSQL container:
```bash
docker-compose up -d
```

3. Check if the container is running:
```bash
docker-compose ps
```

4. View logs to confirm successful initialization:
```bash
docker-compose logs postgres
```

### Connecting to the Database

#### Using psql (from within the container)

```bash
docker exec -it postgres-counter psql -U counter_user -d counter_db
```

#### Using psql (from host machine, if psql installed)

```bash
psql -h localhost -p 5432 -U counter_user -d counter_db
```
Password: `counter_password`

#### Connection Parameters

- **Host**: localhost
- **Port**: 5432
- **Database**: counter_db
- **Username**: counter_user
- **Password**: counter_password

### Verifying the Setup

Once connected to the database, run:

```sql
-- View table structure
\d user_counter

-- View current data
SELECT * FROM user_counter;

-- Test insert
INSERT INTO user_counter (user_id, counter, version) VALUES (100, 0, 0);

-- Test update
UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1;

-- Verify update
SELECT * FROM user_counter WHERE user_id = 1;
```

## Data Persistence

- All database data is stored in the `postgres_data/` directory
- This directory is mounted as a Docker volume, ensuring data persists even if:
  - The container is stopped
  - The container is removed
  - The system reboots

## Managing the Database

### Stop the database
```bash
docker-compose stop
```

### Start the database (after stopping)
```bash
docker-compose start
```

### Restart the database
```bash
docker-compose restart
```

### Stop and remove the container (data persists in postgres_data/)
```bash
docker-compose down
```

### Stop and remove everything including data (⚠️ WARNING: This deletes all data)
```bash
docker-compose down -v
rm -rf postgres_data/
```

## Testing Race Condition Protection

The table is now ready for implementing and testing different counter update strategies:

1. **Simple UPDATE** - Basic increment (may lose updates)
2. **SELECT FOR UPDATE** - Row-level locking
3. **Optimistic Locking** - Using the version field
4. **Stored Procedures** - Server-side logic
5. **Advisory Locks** - PostgreSQL specific locks

These implementations will be developed in subsequent parts of the project.

## Troubleshooting

### Port 5432 already in use

If you have another PostgreSQL instance running:
- Stop the other instance, OR
- Change the port in `docker-compose.yml` (e.g., `"5433:5432"`)

### Permission issues with postgres_data folder

On Linux/Mac, you may need to adjust permissions:
```bash
sudo chown -R 999:999 postgres_data/
```

### Container won't start

Check logs:
```bash
docker-compose logs
```

## Next Steps

Once Part 01 is complete, proceed with implementing different counter update strategies and performance benchmarking.
