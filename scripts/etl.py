import pandas as pd
from sqlalchemy import create_engine

def run_etl():
    # 1. Read CSV
    df = pd.read_csv("/opt/airflow/data/employees.csv")

    # 2. Basic transformation
    df["salary"] = df["salary"] * 1.1  # simple bonus logic

    # 3. Connect to PostgreSQL
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

    # 4. Load into DB
    df.to_sql("employees", engine, if_exists="replace", index=False)

    print("ETL completed successfully")

if __name__ == "__main__":
    run_etl()
