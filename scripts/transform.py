def apply_transformations(df, transformations):
    if not transformations:
        return df

    for t in transformations:
        if t["type"] == "multiply":
            col = t["column"]
            val = t["value"]
            df[col] = df[col] * val

    return df
