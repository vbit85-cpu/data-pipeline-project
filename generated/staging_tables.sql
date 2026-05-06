-- Auto-generated staging DDL
-- Source: configs/mappings.yaml
-- Do not edit manually unless you know what you are doing.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Staging table for: entities
CREATE TABLE IF NOT EXISTS stg_entities (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    source_id INT,
    ent_name TEXT
);

-- Staging table for: agents
CREATE TABLE IF NOT EXISTS stg_agents (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    source_id INT,
    parent_id INT,
    ag_name TEXT
);

-- Staging table for: users
CREATE TABLE IF NOT EXISTS stg_users (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    source_id INT,
    username TEXT,
    email TEXT
);

-- Staging table for: employees
CREATE TABLE IF NOT EXISTS stg_employees (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    source_id INT,
    name TEXT,
    department TEXT,
    salary DOUBLE PRECISION
);

-- Staging table for: orders
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

-- Staging table for: incomes
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
