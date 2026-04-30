import yaml
import os
from datetime import datetime

def save_bad_records(df, name):
    os.makedirs("data/bad", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"data/bad/{name}_bad_{timestamp}.csv"

    #path = f"data/bad/{name}_bad.csv"
    df.to_csv(path, index=False)

def load_config(path="configs/mappings.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)
