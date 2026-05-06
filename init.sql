CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS etl_quality_checks (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    table_name TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    severity TEXT NOT NULL,
    failed_count INT DEFAULT 0,
    details TEXT,
    checked_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS etl_batches (
    batch_id UUID PRIMARY KEY,
    started_at TIMESTAMP NOT NULL DEFAULT now(),
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS agents (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT UNIQUE NOT NULL,
    parent_id INT,
    ag_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT UNIQUE NOT NULL,
    username    TEXT NOT NULL,
    email       TEXT
);

CREATE TABLE IF NOT EXISTS entities (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT UNIQUE NOT NULL,
    ent_name TEXT
);

CREATE TABLE IF NOT EXISTS employees (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT UNIQUE NOT NULL,
    name TEXT,
    department TEXT,
    salary DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS incomes (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT UNIQUE NOT NULL,
    ent_id INT REFERENCES entities(id),
    ag_id INT REFERENCES agents(id),
    income_qty DOUBLE PRECISION,
    income_price DOUBLE PRECISION,
    income_sum DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS orders (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT UNIQUE NOT NULL,
    ent_id INT REFERENCES entities(id),
    ag_id INT REFERENCES agents(id),
    ag_ch_id INT REFERENCES agents(id),
    order_qty DOUBLE PRECISION,
    price DOUBLE PRECISION,
    order_sum DOUBLE PRECISION
);
