import pandas as pd

TYPE_MAP = {
    "int": int,
    "float": float,
    "string": str
}


# --- 1. Проверка структуры (DataFrame уровень) ---
def validate_schema(df: pd.DataFrame, config: dict):
    errors = []

    expected_cols = config["columns"]

    # Проверка наличия колонок
    for col in expected_cols:
        if col not in df.columns:
            errors.append(f"Missing column: {col}")

    # Проверка лишних колонок
    for col in df.columns:
        if col not in expected_cols:
            errors.append(f"Unexpected column: {col}")

    return errors


# --- 2. Проверка одной строки ---
def validate_and_cast_row(row, columns_config):
    clean_row = {}
    errors = []

    for col, rules in columns_config.items():
        value = row.get(col)

        # required
        if rules.get("required") and (pd.isnull(value) or value == ""):
            errors.append(f"{col} is null")
            continue

        # если не required и пусто
        if pd.isnull(value) or value == "":
            clean_row[col] = None
            continue

        # тип
        expected_type = rules.get("type")
        caster = TYPE_MAP.get(expected_type)

        if caster:
            try:
                if expected_type == "int":
                    value = int(float(value))
                elif expected_type == "float":
                    value = float(value)
                elif expected_type == "string":
                    value = str(value)

                clean_row[col] = value

            except Exception:
                errors.append(f"{col} invalid type: {value}")
        else:
            clean_row[col] = value

    return clean_row, errors


# --- 3. Разделение valid / invalid ---
def split_valid_invalid(df: pd.DataFrame, config: dict):
    valid_rows = []
    invalid_rows = []

    columns_config = config["columns"]

    for _, row in df.iterrows():
        clean_row, row_errors = validate_and_cast_row(row, columns_config)

        if row_errors:
            clean_row["_errors"] = "; ".join(row_errors)
            invalid_rows.append(clean_row)
        else:
            valid_rows.append(clean_row)

    valid_df = pd.DataFrame(valid_rows)
    invalid_df = pd.DataFrame(invalid_rows)

    return valid_df, invalid_df
