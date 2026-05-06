import pandas as pd

def extract_csv(path):
    return pd.read_csv(path)

def normalize_columns(df, cfg):
    df = df.copy()

    rename_map = {}

    primary_key = cfg.get("primary_key")
    if primary_key:
        rename_map[primary_key] = "source_id"

    for target_col, meta in cfg["columns"].items():
        source_col = meta.get("source")

        if source_col:
            rename_map[source_col] = target_col

    df.rename(columns=rename_map, inplace=True)

    return df

def apply_types(df, cfg):
    df = df.copy()

    for col, meta in cfg["columns"].items():
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found after PK normalization")

        col_type = meta.get("type")

        if col_type == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif col_type == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")

        elif col_type == "string":
            df[col] = df[col].astype("string")

        else:
            raise ValueError(f"Unsupported type '{col_type}' for column '{col}'")

    return df
