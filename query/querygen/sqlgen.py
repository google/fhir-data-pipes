import os

import pandas as pd
import stanza
from google import genai
from sqlalchemy import engine, text

from . import PRINT_CLOSE_CONCEPTS, PRINT_FINAL_PROMPT, PRINT_MODEL_RESPONSE
from .column_sampler import ColumnSampler
from .db_description import DbDescription
from .terminology_indexer import (CLOSE_CONCEPT_DISTANCE_THRESHOLD,
                                  MAX_CLOSE_CONCEPTS, TerminologyIndexer)

_SQL_START_TOKEN = "--- BEGIN"
_SQL_END_TOKEN = "--- END"
_MAX_ERROR_LENGTH = 1000


def _extract_sql(model_response: str) -> str:
    last_start_ind = model_response.rfind(_SQL_START_TOKEN)
    if last_start_ind < 0:
        return None
    last_end_ind = model_response.rfind(_SQL_END_TOKEN, last_start_ind)
    if last_end_ind < 0:
        return None
    return model_response[last_start_ind + len(_SQL_START_TOKEN) : last_end_ind]


class SqlGen:

    def __init__(
        self,
        target_db_url: str,
        pg_vector_db_url: str,
        nlp_ner: stanza.Pipeline = None,
        num_column_samples: int = 3,
        close_threshold: float = CLOSE_CONCEPT_DISTANCE_THRESHOLD,
        max_close: int = MAX_CLOSE_CONCEPTS,
    ):
        """
        :param target_db_url: main DB URL, e.g., 'hive://localhost:10001/default'
        :param pg_vector_db_url: postgres vector DB URL, e.g.,
            'postgresql://postgres:admin@localhost:5438/codevec'
        """
        self._target_db = engine.create_engine(target_db_url)
        self._vec_db = engine.create_engine(pg_vector_db_url)
        self._column_sampler = ColumnSampler(
            self._target_db, num_samples=num_column_samples
        )
        self._indexer = TerminologyIndexer(target_db_url, pg_vector_db_url)
        self._close_threshold = close_threshold
        self._max_close = max_close
        if not os.environ["GOOGLE_CLOUD_PROJECT"]:
            raise RuntimeError("Env variable GOOGLE_CLOUD_PROJECT is not set!")
        if not os.environ["GOOGLE_CLOUD_LOCATION"]:
            raise RuntimeError("Env variable GOOGLE_CLOUD_LOCATION is not set!")
        self._client = genai.Client()
        if nlp_ner:
            self._nlp_ner = nlp_ner
        else:
            self._nlp_ner = stanza.Pipeline(
                lang="en",
                processors="tokenize,ner",
                package={
                    "ner": [
                        "ncbi_disease",
                        "i2b2",
                        "radiology",
                        "ontonotes-ww-multi_charlm",
                    ]
                },
            )
        self._query_history = []
        self._last_prompt = ""
        self._last_response = ""

    def _find_close_concepts(self, query: str) -> str:
        ent_close = {}
        query_ner = self._nlp_ner(query)
        for ent in query_ner.ents:
            # TODO consider entity types too
            ent_close[ent.text] = self._indexer.close_concepts(
                ent.text, threshold=self._close_threshold, max_close=self._max_close
            )[["table_name", "code_column_name", "code", "display"]]
        all_concepts = ""
        for e, cl_df in ent_close.items():
            all_concepts += f"""
            For "{e}" this is the list of relevant codes and their description, the format
            of list elements is ('table_name', 'code_column_name', 'code', 'display'):
            {list(cl_df.itertuples(index=False, name=None))}\n
            """
        return all_concepts

    def generate_sql(self, query: str, table_suffix: str = "") -> str:
        db_desc = DbDescription(self._target_db).db_description(table_suffix)
        all_sample_values = self._column_sampler.all_tables_samples(table_suffix)
        all_close_concepts = self._find_close_concepts(query)
        # TODO general the prompt and other DB related parts to get the dialect as input.
        prompt = f"""
Assume that we have a database with these tables and schema:
{db_desc}

Here are some sample values from these tables:
{all_sample_values}

Answer this query by creating a single SQL statement:
{query}

Some of the values in this database that might be relevant to this query are:
{all_close_concepts}

The answer should be a single Spark SQL query with two comments BEGIN and END before and after it, and
each constraint in a separate line with the reason for that constraint added as a comment at the end of
the line, for example:
{_SQL_START_TOKEN}
SELECT COUNT(*)
FROM my_table
WHERE year > 2000; -- Because the year should be greater than 2000
{_SQL_END_TOKEN}
"""
        if PRINT_CLOSE_CONCEPTS:
            print(f"CLOSE CONCEPTS: \n{all_close_concepts}")
        return self._generate_and_save(prompt)

    def _generate_and_save(self, prompt: str) -> str:
        if PRINT_FINAL_PROMPT:
            print(f"PROMPT: \n{prompt}")

        response = self._client.models.generate_content(
            model="gemini-2.0-flash", contents=prompt
        )
        self._last_prompt = prompt
        self._last_response = response.text
        if PRINT_MODEL_RESPONSE:
            print(f"MODEL RESPONSE: \n{self._last_response}")
        return self._last_response

    def iterative_gen_sql(
        self, query: str, table_suffix: str = "", num_rounds: int = 3
    ):
        self.generate_sql(query, table_suffix)
        i = 0
        while i < num_rounds:
            last_sql = _extract_sql(self._last_response)
            # Note last_sql can be `None`.
            print(f"Iteration {i} SQL: \n" + str(last_sql))
            # We avoid using pd.read_sql_query to have better control on errors.
            sql_output = ""
            if last_sql:
                with self._target_db.connect() as connection:
                    try:
                        result = connection.execute(text(last_sql))
                        sql_output = (
                            "START_OUTPUT\n"
                            + "\n".join(str(row) for row in result)
                            + "\nEND_OUTPUT"
                        )
                    except Exception as err:
                        sql_output = (
                            "START_ERROR\n"
                            + str(err)[:_MAX_ERROR_LENGTH]
                            + "\nEND_ERROR"
                        )
            print(f"Iteration {i} SQL execution output: \n" + sql_output)
            i += 1
            if i < num_rounds:
                prompt = (
                    self._last_prompt
                    + f"""
For this SQL:
{_SQL_START_TOKEN}
{last_sql}
{_SQL_END_TOKEN}

Database output (or error) was:
{sql_output}

Now generate a SQL, fixing any issues of the previous ones, to answer the same query.
"""
                )
                self._generate_and_save(prompt)


if __name__ == "__main__":
    my_instance = SqlGen(
        target_db_url="hive://localhost:10001/default",
        pg_vector_db_url="postgresql://postgres:admin@localhost:5438/codevec",
    )
    sql_query = my_instance.generate_sql(
        query="""
        During the year 2130, how many patients have been admitted to the emergency department that
        have had a fever, i.e., a body temperature higher than 37?
        """
    )
    print(sql_query)
