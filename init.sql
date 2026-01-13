-- Initialize the database and create the user_counter table

-- Connect to the counter_db database
\c counter_db;

-- Create the user_counter table
CREATE TABLE IF NOT EXISTS user_counter (
    user_id INTEGER PRIMARY KEY,
    counter INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 0
);

-- Create an index on user_id for faster lookups (already indexed as PRIMARY KEY, but explicit for clarity)
-- CREATE INDEX IF NOT EXISTS idx_user_counter_user_id ON user_counter(user_id);

-- Insert some sample data for testing (optional)
INSERT INTO user_counter (user_id, counter, version) 
VALUES 
    (1, 0, 0),
    (2, 0, 0),
    (3, 0, 0)
ON CONFLICT (user_id) DO NOTHING;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON TABLE user_counter TO counter_user;

-- Display the created table structure
\d user_counter;

-- Display sample data
SELECT * FROM user_counter;
