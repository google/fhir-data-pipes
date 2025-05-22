from sqlalchemy import engine
import pandas as pd


def list_all_tables(con: engine.Engine) -> list[str]:
    return pd.read_sql_query(
        sql="SHOW TABLES;",  # TODO make this generic for SQL dialects.
        con=con)['tableName'].astype(str).tolist()
