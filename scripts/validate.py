import pandas as pd

TYPE_MAP = {
    "int": int,
    "float": float,
    "string": str
}


# ---1. Structure check (DataFrame level) ---
def validate_schema(df: pd.DataFrame, config: dict):
    errors = []

    expected_cols = config["columns"]

    # Checking the presence of columns
    for col in expected_cols:
        if col not in df.columns:
            errors.append(f"Missing column: {col}")

    # Checking extra columns
    for col in df.columns:
        if col not in expected_cols:
            errors.append(f"Unexpected column: {col}")

    return errors


# --- 2. Checking one line ---
def validate_and_cast_row(row, columns_config):
    clean_row = {}
    errors = []

    for col, rules in columns_config.items():
        value = row.get(col)

        # required
        if rules.get("required") and (pd.isnull(value) or value == ""):
            errors.append(f"{col} is null")
            continue

        # if not required and empty
        if pd.isnull(value) or value == "":
            clean_row[col] = None
            continue

        # type
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


def split_valid_invalid_old(df: pd.DataFrame, config: dict):
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

def split_valid_invalid(df: pd.DataFrame, config: dict):
    valid_rows = []
    invalid_rows = []

    columns_config = config["columns"]

    for _, row in df.iterrows():
        errors = []

        for column, rules in columns_config.items():
            required = rules.get("required", False)

            if required and pd.isna(row[column]):
                errors.append(f"{column} is required or has invalid type")

        row_dict = row.to_dict()

        if errors:
            row_dict["_errors"] = "; ".join(errors)
            invalid_rows.append(row_dict)
        else:
            valid_rows.append(row_dict)

    valid_df = pd.DataFrame(valid_rows)
    invalid_df = pd.DataFrame(invalid_rows)

    return valid_df, invalid_df
