from __future__ import annotations
import json
from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

class IngestionRepository:
    def __init__(self, spark: SparkSession, table: str):
        self.spark, self.table = spark, table

    def get_recent_failures(self, hours: int) -> list[dict[str, Any]]:
        rows = (self.spark.table(self.table)
            .where((F.col("status") == "FAILED") &
                   (F.col("finished_at") >= F.current_timestamp() - F.expr(f"INTERVAL {hours} HOURS")))
            .collect())
        return [r.asDict(recursive=True) for r in rows]

class OccurrenceRepository:
    def __init__(self, spark: SparkSession, table: str):
        self.spark, self.table = spark, table

    @staticmethod
    def schema():
        action = StructType([
            StructField("action_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("action_code", StringType(), False),
            StructField("runbook_code", StringType(), True),
            StructField("status", StringType(), False),
            StructField("success", BooleanType(), False),
            StructField("attempt", IntegerType(), False),
            StructField("started_at", TimestampType(), False),
            StructField("finished_at", TimestampType(), True),
            StructField("message", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("details", MapType(StringType(), StringType()), True),
        ])
        return StructType([
            StructField("occurrence_id", StringType(), False),
            StructField("occurrence_key", StringType(), False),
            StructField("database_name", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("failure_code", StringType(), False),
            StructField("runbook_code", StringType(), True),
            StructField("action_code", StringType(), True),
            StructField("status", StringType(), False),
            StructField("first_detected_at", TimestampType(), False),
            StructField("last_detected_at", TimestampType(), False),
            StructField("attempt_count", IntegerType(), False),
            StructField("execution_id", StringType(), False),
            StructField("error_message", StringType(), True),
            StructField("resolved_at", TimestampType(), True),
            StructField("actions", ArrayType(action, containsNull=False), False),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ])

    def exists(self) -> bool:
        return self.spark.catalog.tableExists(self.table)

    def find_by_key(self, key: str) -> dict[str, Any] | None:
        if not self.exists():
            return None
        rows = self.spark.table(self.table).where(F.col("occurrence_key") == key).limit(1).collect()
        return rows[0].asDict(recursive=True) if rows else None

    def save(self, row: dict[str, Any]) -> None:
        df = self.spark.createDataFrame([row], schema=self.schema())
        if not self.exists():
            df.write.format("delta").mode("overwrite").saveAsTable(self.table)
            return
        df.createOrReplaceTempView("_incoming_occurrence")
        self.spark.sql(f"""
            MERGE INTO {self.table} t
            USING _incoming_occurrence s
              ON t.occurrence_key = s.occurrence_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

class RunbookRepository:
    def __init__(self, spark: SparkSession, table: str):
        self.spark, self.table = spark, table

    def find_candidates(self, failure_code: str) -> list[dict[str, Any]]:
        rows = (self.spark.table(self.table)
            .where((F.col("failure_code") == failure_code) & (F.col("enabled") == True))
            .orderBy(F.col("priority").asc())
            .collect())
        return [r.asDict(recursive=True) for r in rows]
