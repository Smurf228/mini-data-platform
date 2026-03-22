from pathlib import Path
import os

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


SCHEMA = "demo"
DATA_DIR = Path("data")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "55432"))
DB_NAME = os.getenv("DB_NAME", "appdb")
DB_USER = os.getenv("DB_USER", "app")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")


def infer_postgres_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    return "TEXT"


def detect_primary_key(columns: list[str]) -> str | None:
    for column in columns:
        if column.lower() == "id" or column.lower().endswith("_id"):
            return column
    return None


def existing_columns(cursor, schema: str, table: str) -> list[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [row[0] for row in cursor.fetchall()]


def create_table(cursor, schema: str, table: str, dataframe: pd.DataFrame, primary_key: str | None) -> None:
    columns_sql = []
    for column in dataframe.columns:
        columns_sql.append(
            sql.SQL("{} {}").format(sql.Identifier(column), sql.SQL(infer_postgres_type(dataframe[column])))
        )

    if primary_key:
        columns_sql.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(primary_key)))

    create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(columns_sql),
    )
    cursor.execute(create_sql)


def upsert_dataframe(cursor, schema: str, table: str, dataframe: pd.DataFrame, primary_key: str | None) -> None:
    columns = list(dataframe.columns)
    values = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]

    base_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )

    if primary_key:
        update_columns = [col for col in columns if col != primary_key]
        if update_columns:
            upsert_query = base_query + sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {} ").format(
                sql.Identifier(primary_key),
                sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                    for col in update_columns
                ),
            )
        else:
            upsert_query = base_query + sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(sql.Identifier(primary_key))
    else:
        upsert_query = base_query

    execute_values(cursor, upsert_query, values)


def main() -> None:
    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("No CSV files found in data directory")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    cur = conn.cursor()

    try:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {} ").format(sql.Identifier(SCHEMA)))

        for csv_file in csv_files:
            table_name = csv_file.stem
            dataframe = pd.read_csv(csv_file)
            dataframe = dataframe.where(pd.notnull(dataframe), None)

            if dataframe.empty:
                print(f"Skipping empty file: {csv_file.name}")
                continue

            pk = detect_primary_key(list(dataframe.columns))
            current_columns = existing_columns(cur, SCHEMA, table_name)
            expected_columns = list(dataframe.columns)

            # Recreate table when columns drift from CSV contract.
            if current_columns and current_columns != expected_columns:
                cur.execute(
                    sql.SQL("DROP TABLE {}.{} CASCADE").format(
                        sql.Identifier(SCHEMA),
                        sql.Identifier(table_name),
                    )
                )

            create_table(cur, SCHEMA, table_name, dataframe, pk)
            upsert_dataframe(cur, SCHEMA, table_name, dataframe, pk)
            print(f"Loaded {len(dataframe)} rows into {SCHEMA}.{table_name}")

        conn.commit()
    finally:
        cur.close()
        conn.close()

    print("Dynamic CSV load completed successfully")


if __name__ == "__main__":
    main()
