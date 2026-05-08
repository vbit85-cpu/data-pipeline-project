-- Auto-generated staging DDL
-- Source: configs/mappings.yaml
-- Do not edit manually unless you know what you are doing.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Staging table for: entities
CREATE TABLE IF NOT EXISTS stg_entities (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    raw_record JSONB,
    source_id TEXT,
    ent_name TEXT,
    CONSTRAINT uq_stg_entities_batch_file_row
    UNIQUE (batch_id, source_file, source_row_number)
);

-- Staging table for: agents
CREATE TABLE IF NOT EXISTS stg_agents (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    raw_record JSONB,
    source_id TEXT,
    parent_id TEXT,
    ag_name TEXT,
    CONSTRAINT uq_stg_agents_batch_file_row
    UNIQUE (batch_id, source_file, source_row_number)
);

-- Staging table for: users
CREATE TABLE IF NOT EXISTS stg_users (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    raw_record JSONB,
    source_id TEXT,
    username TEXT,
    email TEXT,
    CONSTRAINT uq_stg_users_batch_file_row
    UNIQUE (batch_id, source_file, source_row_number)
);

-- Staging table for: employees
CREATE TABLE IF NOT EXISTS stg_employees (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    raw_record JSONB,
    source_id TEXT,
    name TEXT,
    department TEXT,
    salary TEXT,
    CONSTRAINT uq_stg_employees_batch_file_row
    UNIQUE (batch_id, source_file, source_row_number)
);

-- Staging table for: orders
CREATE TABLE IF NOT EXISTS stg_orders (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    raw_record JSONB,
    source_id TEXT,
    ent_id TEXT,
    order_qty TEXT,
    price TEXT,
    order_sum TEXT,
    ag_id TEXT,
    ag_ch_id TEXT,
    CONSTRAINT uq_stg_orders_batch_file_row
    UNIQUE (batch_id, source_file, source_row_number)
);

-- Staging table for: incomes
CREATE TABLE IF NOT EXISTS stg_incomes (
    stg_id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    loaded_at TIMESTAMP DEFAULT now(),
    raw_record JSONB,
    source_id TEXT,
    ent_id TEXT,
    ag_id TEXT,
    income_qty TEXT,
    income_price TEXT,
    income_sum TEXT,
    CONSTRAINT uq_stg_incomes_batch_file_row
    UNIQUE (batch_id, source_file, source_row_number)
);
