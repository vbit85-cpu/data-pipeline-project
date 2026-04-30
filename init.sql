CREATE TABLE IF NOT EXISTS employees (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    name TEXT,
    department TEXT,
    salary DOUBLE PRECISION,
    UNIQUE (source_id)
);

CREATE TABLE IF NOT EXISTS incomes (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    ent_id INT,
    ag_id INT,
    income_qty DOUBLE PRECISION,
    income_price DOUBLE PRECISION,
    income_sum DOUBLE PRECISION,
    UNIQUE (source_id)
);

CREATE TABLE IF NOT EXISTS orders (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    ent_id INT,
    order_qty DOUBLE PRECISION,
    price DOUBLE PRECISION,
    order_sum DOUBLE PRECISION,
    ag_id INT,
    ag_ch_id INT,
    UNIQUE (source_id)
);

CREATE TABLE IF NOT EXISTS agents (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    parent_id INT,
    ag_name TEXT,
    UNIQUE(source_id)
);

CREATE TABLE IF NOT EXISTS users (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    userName    TEXT NOT NULL,
    Email       TEXT NOT NULL,
    UNIQUE(source_id)
);

CREATE TABLE IF NOT EXISTS entities (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    ent_name TEXT,
    UNIQUE(source_id)
);

CREATE TABLE IF NOT EXISTS stg_employees (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    name TEXT,
    department TEXT,
    salary DOUBLE PRECISION,
    UNIQUE (source_id)
);

CREATE TABLE IF NOT EXISTS stg_incomes (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    ent_id INT,
    ag_id INT,
    income_qty DOUBLE PRECISION,
    income_price DOUBLE PRECISION,
    income_sum DOUBLE PRECISION,
    UNIQUE (source_id)
);

CREATE TABLE IF NOT EXISTS stg_orders (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    ent_id INT,
    order_qty DOUBLE PRECISION,
    price DOUBLE PRECISION,
    order_sum DOUBLE PRECISION,
    ag_id INT,
    ag_ch_id INT,
    UNIQUE (source_id)
);

CREATE TABLE IF NOT EXISTS stg_agents (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    parent_id INT,
    ag_name TEXT,
    UNIQUE(source_id)
);

CREATE TABLE IF NOT EXISTS stg_users (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    userName    TEXT NOT NULL,
    Email       TEXT NOT NULL,
    UNIQUE(source_id)
);

CREATE TABLE IF NOT EXISTS stg_entities (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    ent_name TEXT,
    UNIQUE(source_id)
);
