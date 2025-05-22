from sqlalchemy import engine
import pandas as pd
from .util import list_all_tables
from . import PRINT_DB_DESC


class DbDescription:
    def __init__(self, target_db_url: engine.Engine):
        self._target_db = target_db_url

    def db_description(self, table_name_suffix='') -> str:
        db_desc = ''
        selected_tables = [t for t in list_all_tables(self._target_db)
                           if t.endswith(table_name_suffix)]
        for t in selected_tables:
            db_desc += self.table_description(t)
        if PRINT_DB_DESC:
            print(f'FINAL DB DESCRIPTION: \n{db_desc}')
        return db_desc

    def table_description(self, table_name: str) -> str:
        desc = f'\nTable {table_name} columns and types are: \n'
        for r in pd.read_sql_query(
                sql=f"DESCRIBE {table_name};",
                con=self._target_db)[['col_name', 'data_type']].values:
            # desc += f'Column {r[0]} has type {r[1]}\n'
            desc += f'{r[0]}: {r[1]}\n'
        if PRINT_DB_DESC:
            print(f'TABLE DESCRIPTION: \n{desc}')
        return desc
