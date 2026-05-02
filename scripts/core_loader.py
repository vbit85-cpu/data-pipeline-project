from utils import get_connection


def _execute(conn, query, table_name, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params)
        affected = cur.rowcount
    conn.commit()
    print(f"[CORE LOAD] {table_name}: {affected} rows affected")


def check_duplicates(conn, staging_table, key_column="source_id"):
    query = f"""
        SELECT {key_column}, COUNT(*)
        FROM {staging_table}
        GROUP BY {key_column}
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC;
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    if rows:
        print(f"[DUPLICATES] {staging_table}: {len(rows)} duplicated keys found")
        for row in rows[:10]:
            print(f"  {key_column}={row[0]}, count={row[1]}")

    return rows


def check_missing_references(conn, staging_table, source_column, ref_table, ref_source_column="source_id"):
    query = f"""
        SELECT s.{source_column}, COUNT(*)
        FROM {staging_table} s
        LEFT JOIN {ref_table} r
            ON s.{source_column} = r.{ref_source_column}
        WHERE r.{ref_source_column} IS NULL
        GROUP BY s.{source_column}
        ORDER BY COUNT(*) DESC;
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    if rows:
        print(
            f"[MISSING REFERENCES] {staging_table}.{source_column} "
            f"→ {ref_table}.{ref_source_column}: {len(rows)} missing keys"
        )
        for row in rows[:10]:
            print(f"  missing {source_column}={row[0]}, count={row[1]}")

    return rows


def load_entities_core(conn, batch_id):
    check_duplicates(conn, "stg_entities")

    query = """
        WITH latest AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                ent_name
            FROM stg_entities
            WHERE source_id IS NOT NULL
              AND batch_id = %s
            ORDER BY source_id, loaded_at DESC, stg_id DESC
        )
        INSERT INTO entities (source_id, ent_name)
        SELECT source_id, ent_name
        FROM latest
        ON CONFLICT (source_id) DO UPDATE
        SET ent_name = EXCLUDED.ent_name;
    """

    _execute(conn, query, "entities", (str(batch_id),))


def load_agents_core(conn, batch_id):
    check_duplicates(conn, "stg_agents")

    insert_query = """
        WITH latest AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                parent_id,
                ag_name
            FROM stg_agents
            WHERE source_id IS NOT NULL
              AND batch_id = %s
            ORDER BY source_id, loaded_at DESC, stg_id DESC
        )
        INSERT INTO agents (source_id, parent_id, ag_name)
        SELECT
            source_id,
            NULL AS parent_id,
            ag_name
        FROM latest
        ON CONFLICT (source_id) DO UPDATE
        SET
            ag_name = EXCLUDED.ag_name;
    """

    update_parent_query = """
        WITH latest AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                parent_id
            FROM stg_agents
            WHERE source_id IS NOT NULL
              AND batch_id = %s
            ORDER BY source_id, loaded_at DESC, stg_id DESC
        )
        UPDATE agents child
        SET parent_id = parent.id
        FROM latest l
        LEFT JOIN agents parent
            ON l.parent_id = parent.source_id
        WHERE child.source_id = l.source_id;
    """

    _execute(conn, insert_query, "agents insert", (str(batch_id),))
    _execute(conn, update_parent_query, "agents parent_id update", (str(batch_id),))


def load_users_core(conn, batch_id):
    check_duplicates(conn, "stg_users")

    query = """
        WITH latest AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                userName,
                Email
            FROM stg_users
            WHERE source_id IS NOT NULL
              AND batch_id = %s
            ORDER BY source_id, loaded_at DESC, stg_id DESC
        )
        INSERT INTO users (source_id, userName, Email)
        SELECT source_id, userName, Email
        FROM latest
        ON CONFLICT (source_id) DO UPDATE
        SET
            userName = EXCLUDED.userName,
            Email = EXCLUDED.Email;
    """

    _execute(conn, query, "users", (str(batch_id),))


def load_employees_core(conn, batch_id):
    check_duplicates(conn, "stg_employees")

    query = """
        WITH latest AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                name,
                department,
                salary
            FROM stg_employees
            WHERE source_id IS NOT NULL
              AND batch_id = %s
            ORDER BY source_id, loaded_at DESC, stg_id DESC
        )
        INSERT INTO employees (source_id, name, department, salary)
        SELECT source_id, name, department, salary
        FROM latest
        ON CONFLICT (source_id) DO UPDATE
        SET
            name = EXCLUDED.name,
            department = EXCLUDED.department,
            salary = EXCLUDED.salary;
    """

    _execute(conn, query, "employees", (str(batch_id),))


def load_orders_core(conn, batch_id):
    check_duplicates(conn, "stg_orders")

    missing_entities = check_missing_references(conn, "stg_orders", "ent_id", "entities")
    missing_agents = check_missing_references(conn, "stg_orders", "ag_id", "agents")
    missing_agents_child = check_missing_references(conn, "stg_orders", "ag_ch_id", "agents")

    if missing_entities or missing_agents or missing_agents_child:
        raise ValueError("Cannot load orders core: missing references found")

    invalid_agent_pairs_query = """
        SELECT
           s.source_id,
           s.ag_id,
           s.ag_ch_id
        FROM stg_orders s
        LEFT JOIN agents parent_ag
            ON s.ag_id = parent_ag.source_id
        LEFT JOIN agents child_ag
            ON s.ag_ch_id = child_ag.source_id
        WHERE parent_ag.id IS NULL
           OR child_ag.id IS NULL
           OR child_ag.parent_id IS DISTINCT FROM parent_ag.id;
    """

    with conn.cursor() as cur:
        cur.execute(invalid_agent_pairs_query)
        invalid_pairs = cur.fetchall()

    if invalid_pairs:
        print(f"[INVALID AGENT PAIRS] stg_orders: {len(invalid_pairs)} invalid pairs found")
        for row in invalid_pairs[:10]:
            print(f"  order_source_id={row[0]}, ag_id={row[1]}, ag_ch_id={row[2]}")

        raise ValueError("Cannot load orders core: invalid ag_id/ag_ch_id parent-child pairs found")

    query = """
        WITH latest AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                ent_id,
                order_qty,
                price,
                order_sum,
                ag_id,
                ag_ch_id
            FROM stg_orders
            WHERE source_id IS NOT NULL
              AND batch_id = %s
            ORDER BY source_id, loaded_at DESC, stg_id DESC
        )
        INSERT INTO orders (
            source_id,
            ent_id,
            order_qty,
            price,
            order_sum,
            ag_id,
            ag_ch_id
        )
        SELECT distinct
            l.source_id,
            e.id AS ent_id,
            l.order_qty,
            l.price,
            l.order_sum,
            parent_ag.id AS ag_id,
            child_ag.id AS ag_ch_id
        FROM latest l
        JOIN entities e
            ON l.ent_id = e.source_id
        JOIN agents parent_ag
            ON l.ag_id = parent_ag.source_id
        JOIN agents child_ag
            ON l.ag_ch_id = child_ag.source_id
           AND child_ag.parent_id = parent_ag.id
        ON CONFLICT (source_id) DO UPDATE
        SET
            ent_id = EXCLUDED.ent_id,
            order_qty = EXCLUDED.order_qty,
            price = EXCLUDED.price,
            order_sum = EXCLUDED.order_sum,
            ag_id = EXCLUDED.ag_id,
            ag_ch_id = EXCLUDED.ag_ch_id;
    """

    _execute(conn, query, "orders", (str(batch_id),))

def load_incomes_core(conn, batch_id):
    check_duplicates(conn, "stg_incomes")

    missing_entities = check_missing_references(conn, "stg_incomes", "ent_id", "entities")
    missing_agents = check_missing_references(conn, "stg_incomes", "ag_id", "agents")

    if missing_entities or missing_agents:
        raise ValueError("Cannot load incomes core: missing references found")

    query = """
        WITH latest AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                ent_id,
                ag_id,
                income_qty,
                income_price,
                income_sum
            FROM stg_incomes
            WHERE source_id IS NOT NULL
              AND batch_id = %s
            ORDER BY source_id, loaded_at DESC, stg_id DESC
        )
        INSERT INTO incomes (
            source_id,
            ent_id,
            ag_id,
            income_qty,
            income_price,
            income_sum
        )
        SELECT
            l.source_id,
            e.id AS ent_id,
            a.id AS ag_id,
            l.income_qty,
            l.income_price,
            l.income_sum
        FROM latest l
        JOIN entities e ON l.ent_id = e.source_id
        JOIN agents a ON l.ag_id = a.source_id
        ON CONFLICT (source_id) DO UPDATE
        SET
            ent_id = EXCLUDED.ent_id,
            ag_id = EXCLUDED.ag_id,
            income_qty = EXCLUDED.income_qty,
            income_price = EXCLUDED.income_price,
            income_sum = EXCLUDED.income_sum;
    """

    _execute(conn, query, "incomes", (str(batch_id),))


def load_core_by_table(conn, table_name, batch_id):
    loaders = {
        "entities": load_entities_core,
        "agents": load_agents_core,
        "users": load_users_core,
        "employees": load_employees_core,
        "orders": load_orders_core,
        "incomes": load_incomes_core,
    }

    if table_name not in loaders:
        raise ValueError(f"No core loader defined for table: {table_name}")

    loaders[table_name](conn, batch_id)


def load_core_by_levels(levels, config, batch_id):
    with get_connection() as conn:
        for level in levels:
            for file_name in level:
                table_name = config["files"][file_name]["table"]
                load_core_by_table(conn, table_name, batch_id)
