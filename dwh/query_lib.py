# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""This is the main higher level library to query FHIR resources.

The public interface of this library is intended to be independent of the actual
query engine, e.g., Spark, SQL/BigQuery, etc. The only exception is a single
function that defines the source of the data.
"""

# See https://stackoverflow.com/questions/33533148 why this is needed.
from __future__ import annotations
import enum
import typing as tp
import base
import query_lib_big_query
import query_lib_spark


# This separator is used to merge date and values into one string.
DATE_VALUE_SEPARATOR = "_SeP_"


def merge_date_and_value(d: str, v: tp.Any) -> str:
    return "{}{}{}".format(d, DATE_VALUE_SEPARATOR, v)


class Runner(enum.Enum):
    SPARK = 1
    BIG_QUERY = 2
    # FHIR_SERVER = 3


def patient_query_factory(
    runner: Runner,
    data_source: str,
    code_system: tp.Optional[str] = None,
    **kwargs: tp.Dict[tp.Any, tp.Any],
) -> base.PatientQuery:

    """Returns the right instance of `PatientQuery` based on `data_source`.

    Args:
      runner: The runner to use for making data queries
      data_source: The definition of the source, e.g., directory containing
        Parquet files or a BigQuery dataset.
      kwargs: A dictionary of runner specific parameters.

    Returns:
      The created instance.

    Raises:
      ValueError: When the input `data_source` is malformed or not implemented.
    """
    if runner == Runner.SPARK:
        return query_lib_spark.SparkPatientQuery(data_source, code_system)
    if runner == Runner.BIG_QUERY:
        if "project_name" not in kwargs:
            raise ValueError(
                "'project_name' should be provided in kwargs while using "
                "a BigQuery data source"
            )
        return query_lib_big_query.BigQueryPatientQuery(
            project_name=kwargs['project_name'],
            bq_dataset=data_source,
            code_system=code_system,
        )

    raise ValueError("Query engine {} is not supported yet.".format(runner))
