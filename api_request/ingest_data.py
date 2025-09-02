import time
from tqdm import tqdm  # pip install tqdm
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from dotenv import load_dotenv
load_dotenv()


def get_engine():
    # Construye la URL si no viene completa
    db = os.getenv("POSTGRES_DB", "db")
    user = os.getenv("POSTGRES_USER", "root")
    pwd = os.getenv("POSTGRES_PWD", "root")
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

    return create_engine(url, pool_pre_ping=True)


def ingest_data_to_db(engine, file_path, table_name, schema=None, chunksize=50_000):
    df = pd.read_parquet(file_path)
    n = len(df)
    total_chunks = (n // chunksize) + (1 if n % chunksize else 0)

    start = time.time()
    for i in tqdm(range(total_chunks), desc="Ingesting"):
        chunk = df.iloc[i*chunksize: (i+1)*chunksize]
        chunk.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False,
            schema=schema,
            method='multi'
        )
    elapsed = time.time() - start
    print(
        f"✔ Ingested {n} rows into {schema+'.' if schema else ''}{table_name} in {elapsed:.1f} s")


def main():
    table_name = os.getenv("TABLE_NAME", "titanic_data")
    # mejor pasarla por env o arg
    file_path = os.getenv(
        "DATA_PATH", "/opt/airflow/data/titanic.parquet")
    if not file_path:
        raise ValueError("DATA_PATH no está definido en el entorno.")

    engine = None
    try:
        engine = get_engine()

        ingest_data_to_db(engine, file_path, table_name, schema="public")
    except SQLAlchemyError as e:
        print(f"Operational error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if engine is not None:
            engine.dispose()
            print("Connection pool disposed.")


if __name__ == "__main__":
    main()
