CREATE EXTENSION IF NOT EXISTS pgcrypto;

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
    userName    TEXT NOT NULL,
    Email       TEXT
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

CREATE TABLE IF NOT EXISTS stg_employees (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    source_id INT,
    name TEXT,
    department TEXT,
    salary DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS stg_agents (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    source_id INT,
    parent_id INT,
    ag_name TEXT
);

CREATE TABLE IF NOT EXISTS stg_users (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    source_id INT,
    userName    TEXT,
    Email       TEXT
);

CREATE TABLE IF NOT EXISTS stg_entities (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    source_id INT,
    ent_name TEXT
);

CREATE TABLE IF NOT EXISTS stg_incomes (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    source_id INT,
    ent_id INT,
    ag_id INT,
    income_qty DOUBLE PRECISION,
    income_price DOUBLE PRECISION,
    income_sum DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS stg_orders (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    source_id INT,
    ent_id INT,
    order_qty DOUBLE PRECISION,
    price DOUBLE PRECISION,
    order_sum DOUBLE PRECISION,
    ag_id INT,
    ag_ch_id INT
);

