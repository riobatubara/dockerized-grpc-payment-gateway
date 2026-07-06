-- Create Users Table for Authentication and Role-Based Authorization
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'merchant',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create Transactions Table to log Payment Gateway history
CREATE TABLE IF NOT EXISTS transactions (
    id VARCHAR(100) PRIMARY KEY,
    user_id INT REFERENCES users(id),
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Seed a default user 
-- The password hash below corresponds to the plain text password: 'password123'
-- Generated using bcrypt
INSERT INTO users (username, password_hash, role) 
VALUES ('admin', '$2a$10$dIDo0590nuP1Bjw5yRY.beiJrKqOmZ46z74IMC9B3N3H0E18xGKwW', 'admin')
ON CONFLICT (username) DO NOTHING;
