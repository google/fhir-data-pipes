import pandas as pd
from sqlalchemy import engine


def list_all_tables(con: engine.Engine) -> list[str]:
    return (
        pd.read_sql_query(
            sql="SHOW TABLES;", con=con  # TODO make this generic for SQL dialects.
        )["tableName"]
        .astype(str)
        .tolist()
    )
