import os

import pandas as pd
from google import genai
from google.genai.types import EmbedContentConfig
from sqlalchemy import Column, Integer, MetaData, String, Table, engine

from . import PRINT_CLOSE_CONCEPTS

CLOSE_CONCEPT_DISTANCE_THRESHOLD = 0.7
MAX_CLOSE_CONCEPTS = 20


class TerminologyIndexer:

    def __init__(self, target_db_url: str, pg_vector_db_url: str):
        """
        :param target_db_url: main DB URL, e.g., 'hive://localhost:10001/default'
        :param pg_vector_db_url: postgres vector DB URL, e.g.,
            'postgresql://postgres:admin@localhost:5438/codevec'
        """
        self._target_db = engine.create_engine(target_db_url)
        self._vec_db = engine.create_engine(pg_vector_db_url)
        if not os.environ["GOOGLE_CLOUD_PROJECT"]:
            raise RuntimeError("Env variable GOOGLE_CLOUD_PROJECT is not set!")
        if not os.environ["GOOGLE_CLOUD_LOCATION"]:
            raise RuntimeError("Env variable GOOGLE_CLOUD_LOCATION is not set!")
        self._client = genai.Client()

    def extract_and_embed_all_codes(self, conf):
        all_codes = pd.read_sql_query(
            sql=f"""
            SELECT {conf['code_column']} AS code, {conf['system_column']} AS sys,
                COUNT(*) AS num_rows, {conf['display_column']} AS display
            FROM {conf['table_name']} 
            GROUP BY {conf['code_column']}, {conf['system_column']}, {conf['display_column']}
            ORDER BY num_rows DESC
            """,
            con=self._target_db,
        )
        self._create_embedding_db(conf, all_codes)

    def _create_embedding_db(self, config, codes_df: pd.DataFrame):
        """The input codes_df is expected to have `code`, `sys`, `display` columns.

        For each row, this uses the "text-embedding-005" to embed the `display`
        description into a 768 dimensional space. Then it is inserted into
        the `code_vector` table. The value for other columns come from `config`.
        """
        metadata_core = MetaData()
        code_vector_table = Table(
            "code_vector", metadata_core, autoload_with=self._vec_db
        )

        with self._vec_db.connect() as connection:
            print(f"processing {config}")
            for index, row in codes_df.iterrows():
                code = row["code"]
                sys = row["sys"]
                # TODO: This should be fetched from a terminology server instead.
                display = row["display"]
                # Ignore null values.
                if not code or not display:
                    continue
                if index % 100 == 0:
                    print("item: {} code: {} display: {}".format(index, code, display))
                embedding = self.embed(display)
                # We don't use simple INSERT string statements to avoid issues with escaping values.
                insert_statement = code_vector_table.insert().values(
                    code=code,
                    system=sys,
                    display=display,
                    embedding=embedding,
                    table_name=config["table_name"],
                    code_column_name=config["code_column"],
                    system_column_name=config["system_column"],
                    display_column_name=config["display_column"],
                )
                connection.execute(insert_statement)
                connection.commit()

    def embed(self, concept: str) -> list:  # TODO fix type
        response = self._client.models.embed_content(
            model="text-embedding-005",
            contents=[concept],
            config=EmbedContentConfig(
                # task_type="RETRIEVAL_DOCUMENT",  # Optional
                output_dimensionality=768,  # Optional
                # title="Medical Concepts",  # Optional
            ),
        )
        return response.embeddings[0].values

    def close_concepts(
        self,
        query_concept: str,
        threshold: float = CLOSE_CONCEPT_DISTANCE_THRESHOLD,
        max_close: int = MAX_CLOSE_CONCEPTS,
        table_name: str = None,
    ) -> pd.DataFrame:
        query_embedding = self.embed(query_concept)
        table_cond = f"AND table_name='{table_name}'" if table_name != None else ""
        df = pd.read_sql_query(
            sql=f"""
            SELECT code, system, display, table_name, code_column_name, embedding, embedding <#> '{query_embedding}' AS inner
            FROM code_vector
            WHERE embedding <#> '{query_embedding}' < -{threshold}
            {table_cond}
            ORDER BY embedding <#> '{query_embedding}'
            LIMIT {max_close};
            """,
            con=self._vec_db,
        )
        return df
