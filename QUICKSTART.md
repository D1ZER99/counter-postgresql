# Quick Start Guide - PostgreSQL Counter (Part 01)

## 🚀 Getting Started in 3 Steps

### Step 1: Start PostgreSQL
```bash
cd postgres-insert
docker-compose up -d
```

### Step 2: Verify the Setup (Optional)
Install Python dependencies and run test script:
```bash
pip install -r requirements.txt
python test_connection.py
```

### Step 3: Connect to Database
```bash
docker exec -it postgres-counter psql -U counter_user -d counter_db
```

## 📊 Database Credentials

- **Host**: localhost
- **Port**: 5432
- **Database**: counter_db
- **Username**: counter_user
- **Password**: counter_password

## 🗃️ Table Structure

```sql
Table: user_counter
+----------+---------+-------------+
| Column   | Type    | Description |
+----------+---------+-------------+
| user_id  | INTEGER | Primary Key |
| counter  | INTEGER | Counter val |
| version  | INTEGER | For OCC     |
+----------+---------+-------------+
```

## ✅ Verify Setup

Inside PostgreSQL (after Step 3):
```sql
-- View table
\d user_counter

-- Check data
SELECT * FROM user_counter;

-- Test increment
UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1;
```

## 🛑 Stop Database
```bash
docker-compose stop
```

## 📁 Data Persistence

All data is saved in `postgres_data/` folder on your PC. The database will survive:
- Container restarts
- System reboots
- Container removal (with `docker-compose down`)

## ⚠️ Important Notes

1. **First time setup**: Database initialization happens automatically when you first run `docker-compose up`
2. **Port conflicts**: If port 5432 is already in use, change it in `docker-compose.yml`
3. **Data safety**: Data persists on disk. Only deleted if you run `docker-compose down -v` or manually delete `postgres_data/`

## 🔧 Troubleshooting

**Container won't start?**
```bash
docker-compose logs
```

**Port already in use?**
Edit `docker-compose.yml`, change `"5432:5432"` to `"5433:5432"`

**Can't connect?**
Check if container is running:
```bash
docker-compose ps
```

## ✨ What's Next?

Part 01 is complete! You now have:
- ✅ PostgreSQL running in Docker
- ✅ Data persisted to disk (no data loss on reboot)
- ✅ Table `user_counter` ready for testing
- ✅ Protection against race conditions (will implement in Part 02)

Ready for Part 02: Implementing different counter update strategies!
