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

import typing as tp

import pandas
import pyspark
import pyspark.sql as pyspark_sql
import pyspark.sql.functions as F
import pyspark.sql.types as T

import base
import common


# This separator is used to merge date and values into one string.
DATE_VALUE_SEPARATOR = "_SeP_"


def _merge_date_and_value(d: str, v: tp.Any) -> str:
    return "{}{}{}".format(d, DATE_VALUE_SEPARATOR, v)


class SparkPatientQuery(base.PatientQuery):
    def __init__(self, file_root: str, code_system: str):
        super().__init__(code_system)
        self._file_root = file_root
        self._spark = None
        self._patient_df = None
        self._obs_df = None
        self._flat_obs = None
        self._patient_agg_obs_df = None
        self._enc_df = None

    def _make_sure_spark(self):
        if not self._spark:
            # TODO add the option for using a running Spark cluster.
            conf = (
                pyspark.SparkConf()
                .setMaster("local[10]")
                .setAppName("IndicatorsApp")
                .set("spark.driver.memory", "10g")
                .set("spark.executor.memory", "4g")
                # See: https://spark.apache.org/docs/latest/security.html
                .set("spark.authenticate", "true")
            )
            self._spark = pyspark_sql.SparkSession.builder.config(
                conf=conf
            ).getOrCreate()

    def _make_sure_patient(self):
        if not self._patient_df:
            # Loading Parquet files and flattening only happens once.
            self._patient_df = self._spark.read.parquet(
                self._file_root + "/Patient"
            )
            # TODO create inspection functions
            common.custom_log(
                "Number of Patient resources= {}".format(
                    self._patient_df.count()
                )
            )

    def _make_sure_obs(self):
        if not self._obs_df:
            self._obs_df = self._spark.read.parquet(
                self._file_root + "/Observation"
            )
            common.custom_log(
                "Number of Observation resources= {}".format(
                    self._obs_df.count()
                )
            )
        if not self._flat_obs:
            self._flat_obs = SparkPatientQuery._flatten_obs(
                self._obs_df, self._code_system
            )
            common.custom_log(
                "Number of flattened obs rows = {}".format(
                    self._flat_obs.count()
                )
            )

    def _make_sure_encounter(self):
        if not self._enc_df:
            self._enc_df = self._spark.read.parquet(
                self._file_root + "/Encounter"
            )
            common.custom_log(
                "Number of Encounter resources= {}".format(self._enc_df.count())
            )

    def get_patient_obs_view(
        self, sample_count: tp.Optional[int] = None
    ) -> pandas.DataFrame:
        """See super-class doc."""
        self._make_sure_spark()
        self._make_sure_patient()
        self._make_sure_obs()
        self._make_sure_encounter()
        base_patient_url = "Patient/"
        # Recalculating the rest is needed since the constraints can be updated.
        flat_enc = self._flatten_encounter(
            "Encounter/", force_location_type_columns=False
        )
        # TODO figure where `context` comes from and why.
        join_df = self._flat_obs.join(
            flat_enc, flat_enc.encounterId == self._flat_obs.encounterId
        ).where(self._all_constraints_sql())
        agg_obs_df = SparkPatientQuery._aggregate_patient_codes(join_df)
        common.custom_log(
            "Number of aggregated obs= {}".format(agg_obs_df.count())
        )
        self._patient_agg_obs_df = SparkPatientQuery._join_patients_agg_obs(
            self._patient_df, agg_obs_df, base_patient_url
        )
        common.custom_log(
            "Number of joined patient_agg_obs= {}".format(
                self._patient_agg_obs_df.count()
            )
        )
        # Spark is supposed to automatically cache DFs after shuffle but it seems
        # this is not happening!
        self._patient_agg_obs_df.cache()
        temp_pd_df = self._patient_agg_obs_df.toPandas()
        common.custom_log(
            "patient_obs_view size= {}".format(temp_pd_df.index.size)
        )
        temp_pd_df["last_value"] = temp_pd_df.max_date_value.str.split(
            DATE_VALUE_SEPARATOR, expand=True
        )[1]
        temp_pd_df["first_value"] = temp_pd_df.min_date_value.str.split(
            DATE_VALUE_SEPARATOR, expand=True
        )[1]
        temp_pd_df[
            "last_value_code"
        ] = temp_pd_df.max_date_value_code.str.split(
            DATE_VALUE_SEPARATOR, expand=True
        )[
            1
        ]
        temp_pd_df[
            "first_value_code"
        ] = temp_pd_df.min_date_value_code.str.split(
            DATE_VALUE_SEPARATOR, expand=True
        )[
            1
        ]
        # This is good for debug!
        # return temp_pd_df
        return temp_pd_df[
            [
                "patientId",
                "birthDate",
                "gender",
                "code",
                "num_obs",
                "min_value",
                "max_value",
                "min_date",
                "max_date",
                "first_value",
                "last_value",
                "first_value_code",
                "last_value_code",
            ]
        ]

    def get_patient_encounter_view(
        self,
        force_location_type_columns: bool = True,
        sample_count: tp.Optional[int] = None,
    ) -> pandas.DataFrame:
        """See super-class doc."""
        self._make_sure_spark()
        self._make_sure_patient()
        self._make_sure_encounter()
        flat_enc = self._flatten_encounter(
            "Encounter/", force_location_type_columns
        )
        column_list = ["encPatientId"]
        if self._enc_constraint.has_location() or force_location_type_columns:
            column_list += ["locationId", "locationDisplay"]
        if self._enc_constraint.has_type() or force_location_type_columns:
            column_list += ["encTypeSystem", "encTypeCode"]
        spark_frame = flat_enc.groupBy(column_list).agg(
            F.count("*").alias("num_encounters"),
            F.min("first").alias("firstDate"),
            F.max("last").alias("lastDate"),
        )

        # unpack one element list at this point
        frame = spark_frame.toPandas()
        for col in ["encTypeSystem", "encTypeCode"]:
            if col in frame.columns:
                frame[col] = frame[col].apply(lambda x: x[0] if x else x)
        return frame

    def _flatten_encounter(
        self, base_encounter_url: str, force_location_type_columns: bool = True
    ):
        """Returns a custom flat view of encoutners."""
        # When merging flattened encounters and observations, we need to be careful
        # with flattened columns for encounter type and location and only include
        # them if there is a constraints on them. Otherwise we may end up with a
        # single observation repeated multiple times in the view.
        flat_df = self._enc_df.select(
            "subject", "id", "location", "type", "period"
        ).withColumn(
            "encounterId", F.regexp_replace("id", base_encounter_url, "")
        )
        column_list = [
            F.col("encounterId"),
            F.col("subject.patientId").alias("encPatientId"),
            F.col("period.start").alias("first"),
            F.col("period.end").alias("last"),
        ]
        if self._enc_constraint.has_location() or force_location_type_columns:
            flat_df = flat_df.withColumn(
                "locationFlat", F.explode_outer("location")
            )
            column_list += [
                F.col("locationFlat.location.LocationId").alias("locationId"),
                F.col("locationFlat.location.display").alias("locationDisplay"),
            ]
        if self._enc_constraint.has_type() or force_location_type_columns:
            flat_df = flat_df.withColumn("typeFlat", F.explode_outer("type"))
            column_list += [
                F.col("typeFlat.coding.system").alias("encTypeSystem"),
                F.col("typeFlat.coding.code").alias("encTypeCode"),
            ]
        return flat_df.select(column_list).where(
            self._construct_encounter_constraint(self._enc_constraint)
        )

    @staticmethod
    def _flatten_obs(
        obs: pyspark_sql.DataFrame, code_system: str = None
    ) -> pyspark_sql.DataFrame:
        """Creates a flat version of Observation FHIR resources.

        Note `code_system` is only applied on `code.coding` which is a required
        filed, i.e., it is not applied on `value.codeableConcept.coding`.

        Args:
          obs: A collection of Observation FHIR resources.
          code_system: The code system to be used for filtering `code.coding`.
        Returns:
          A DataFrame with the following columns (note one input observation might
          be repeated, once for each of its codes):
          - `coding` from the input obsservation's `code.coding`
          - `valueCoding` from the input's `value.codeableConcept.coding`
          - `value` from the input's `value`
          - `patientId` from the input's `subject.patientId`
          - `dateTime` from the input's `effective.dateTime`
        """
        sys_str = (
            'coding.system="{}"'.format(code_system)
            if code_system
            else "coding.system IS NULL"
        )
        value_sys_str_base = (
            'valueCoding.system="{}"'.format(code_system)
            if code_system
            else "valueCoding.system IS NULL"
        )
        value_sys_str = "(valueCoding IS NULL OR {})".format(value_sys_str_base)
        merge_udf = F.UserDefinedFunction(_merge_date_and_value, T.StringType())
        return (
            obs.withColumn("coding", F.explode("code.coding"))
            .where(sys_str)
            .withColumn(
                "valueCoding",  # Note valueCoding can be null.
                F.explode_outer("value.codeableConcept.coding"),
            )
            .where(value_sys_str)
            .withColumn(
                "dateAndValue",
                merge_udf(
                    F.col("effective.dateTime"), F.col("value.quantity.value")
                ),
            )
            .withColumn(
                "dateAndValueCode",
                merge_udf(
                    F.col("effective.dateTime"), F.col("valueCoding.code")
                ),
            )
            .select(
                F.col("coding"),
                F.col("valueCoding"),
                F.col("value"),
                F.col("subject.patientId").alias("patientId"),
                F.col("effective.dateTime").alias("dateTime"),
                F.col("dateAndValue"),
                F.col("dateAndValueCode"),
                F.col("encounter.EncounterId").alias("encounterId"),
            )
        )

    @staticmethod
    def _aggregate_patient_codes(
        flat_obs: pyspark_sql.DataFrame,
    ) -> pyspark_sql.DataFrame:
        """Find aggregates for each patientId, conceptCode, and codedValue.

        Args:
            flat_obs: A collection of flattened Observations.
        Returns:
          A DataFrame with the following columns:
        """
        return flat_obs.groupBy(["patientId", "coding"]).agg(
            F.count("*").alias("num_obs"),
            F.min("value.quantity.value").alias("min_value"),
            F.max("value.quantity.value").alias("max_value"),
            F.min("dateTime").alias("min_date"),
            F.max("dateTime").alias("max_date"),
            F.min("dateAndValue").alias("min_date_value"),
            F.max("dateAndValue").alias("max_date_value"),
            F.min("dateAndValueCode").alias("min_date_value_code"),
            F.max("dateAndValueCode").alias("max_date_value_code"),
        )

    @staticmethod
    def _join_patients_agg_obs(
        patients: pyspark_sql.DataFrame,
        agg_obs: pyspark_sql.DataFrame,
        base_patient_url: str,
    ) -> pyspark_sql.DataFrame:
        """Joins a collection of Patient FHIR resources with an aggregated obs set.

        Args:
          patients: A collection of Patient FHIR resources.
          agg_obs: Aggregated observations from `aggregate_all_codes_per_patient()`.
        Returns:
          Same `agg_obs` with corresponding patient information joined.
        """
        flat_patients = (
            patients.select(patients.id, patients.birthDate, patients.gender)
            .withColumn(
                "actual_id", F.regexp_replace("id", base_patient_url, "")
            )
            .select("actual_id", "birthDate", "gender")
        )
        return flat_patients.join(
            agg_obs, flat_patients.actual_id == agg_obs.patientId
        ).select(
            "patientId",
            "birthDate",
            "gender",
            "coding.code",
            "num_obs",
            "min_value",
            "max_value",
            "min_date",
            "max_date",
            "min_date_value",
            "max_date_value",
            "min_date_value_code",
            "max_date_value_code",
        )

    @staticmethod
    def _time_constraint(min_time: str = None, max_time: str = None):
        if not min_time and not max_time:
            return "TRUE"
        cl = []
        if min_time:
            cl.append('dateTime >= "{}"'.format(min_time))
        if max_time:
            cl.append('dateTime <= "{}"'.format(max_time))
        return " AND ".join(cl)

    def _construct_obs_constraint(
        self, obs_constraint: base.ObsConstraints
    ) -> str:
        """This creates a constraint string with WHERE syntax in SQL.

        All of the observation constraints specified by this instance are joined
        together into an `AND` clause.
        """
        cl = [
            self._time_constraint(
                obs_constraint.min_time, obs_constraint.max_time
            )
        ]
        cl.append('coding.code="{}"'.format(obs_constraint.code))
        # We don't need to filter coding.system as it is already done in flattening.
        if obs_constraint.values:
            codes_str = ",".join(
                ['"{}"'.format(v) for v in obs_constraint.values]
            )
            cl.append("valueCoding.code IN ({})".format(codes_str))
            cl.append("valueCoding.system {}".format(obs_constraint.sys_str))
        elif obs_constraint.min_value or obs_constraint.max_value:
            if obs_constraint.min_value:
                cl.append(
                    " value.quantity.value >= {} ".format(
                        obs_constraint.min_value
                    )
                )
            if obs_constraint.max_value:
                cl.append(
                    " value.quantity.value <= {} ".format(
                        obs_constraint.max_value
                    )
                )
        return "({})".format(" AND ".join(cl))

    def _all_obs_constraints(self) -> str:
        if not self._code_constraint:
            if self._include_all_codes:
                return "TRUE"
            return "FALSE"
        constraints_str = " OR ".join(
            [
                self._construct_obs_constraint(constraint)
                for constraint in self._code_constraint.values()
            ]
        )
        if not self._include_all_codes:
            return "({})".format(constraints_str)

        others_str = " AND ".join(
            ['coding.code!="{}"'.format(code) for code in self._code_constraint]
            + [
                self._time_constraint(
                    self._all_codes_min_time, self._all_codes_max_time
                )
            ]
        )
        return "({} OR ({}))".format(constraints_str, others_str)

    def _all_constraints_sql(self) -> str:
        obs_str = self._all_obs_constraints()
        enc_str = (
            "{}".format(
                self._construct_encounter_constraint(self._enc_constraint)
            )
            if self._enc_constraint
            else "TRUE"
        )
        return "{} AND {}".format(obs_str, enc_str)

    @staticmethod
    def _construct_encounter_constraint(
        enc_constraint: base.EncounterConstraints,
    ) -> str:
        """This creates a constraint string with WHERE syntax in SQL."""
        loc_str = "TRUE"
        if enc_constraint.location_id:
            temp_str = ",".join(
                ['"{}"'.format(v) for v in enc_constraint.location_id]
            )
            loc_str = "locationId IN ({})".format(temp_str)
        type_code_str = "TRUE"
        if enc_constraint.type_code:
            temp_str = ",".join(
                ['"{}"'.format(v) for v in enc_constraint.type_code]
            )
            # type_code_str = 'encTypeCode IN ({})'.format(temp_str)
            type_code_str = " arrays_overlap(encTypeCode, array({})) ".format(
                temp_str
            )
        type_sys_str = "TRUE"
        if enc_constraint.type_system:
            type_sys_str = ' array_contains(encTypeSystem,  "{}") '.format(
                enc_constraint.type_system
            )
        return "{} AND {} AND {}".format(loc_str, type_code_str, type_sys_str)
