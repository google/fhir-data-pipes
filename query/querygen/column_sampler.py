import pandas as pd
from sqlalchemy import engine

from . import PRINT_SAMPLE_VALUES
from .util import list_all_tables


class ColumnSampler:

    def __init__(self, target_db_engine: engine.Engine, num_samples: int = 5):
        """
        :param target_db_engine: a sqlalchemy Engine for querying the target DB
        """
        self._target_db = target_db_engine
        self._num_samples = num_samples
        self._cached_value = ""

    def all_tables_samples(self, table_name_suffix="") -> str:
        if not self._cached_value:
            self._cached_value = ""
            selected_tables = [
                t
                for t in list_all_tables(self._target_db)
                if t.endswith(table_name_suffix)
            ]
            for t in selected_tables:
                self._cached_value += self._table_sample(t, self._num_samples)
            if PRINT_SAMPLE_VALUES:
                print(f"DB SAMPLE VALUES: \n{self._cached_value}")
        return self._cached_value

    def _table_sample(self, table_name: str, num_samples: int) -> str:
        sample_values = ""
        for r in pd.read_sql_query(sql=f"DESCRIBE {table_name};", con=self._target_db)[
            ["col_name", "data_type"]
        ].values:
            sample_values += f"""
Here are {num_samples} sample values from column {r[0]} of table {table_name}
sorted by their frequencies over ALL rows: \n
"""
            # We are trying to balance between performance and value of the extracted
            # records; doing the GROUP BY on the whole table can be expensive.
            sample_values += pd.read_sql_query(
                sql=f"""
                SELECT column_value, COUNT(*) AS num_records
                FROM (
                  SELECT {r[0]} AS column_value
                  FROM {table_name}
                  -- For improve speed we can limit number of rows being
                  -- sampled but will hinder quality.
                  -- LIMIT {num_samples * 1000}
                )
                GROUP BY column_value
                ORDER BY num_records DESC
                LIMIT {num_samples}
                ;
                """,
                con=self._target_db,
            ).to_string(header=False)
            sample_values += "\n"
        if PRINT_SAMPLE_VALUES:
            print(f"TABLE SAMPLE VALUES: \n{sample_values}")
        return sample_values
