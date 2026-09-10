from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from data_recovery.repositories import IngestionRepository, OccurrenceRepository, RunbookRepository
from data_recovery.services import MTTRService, RunbookService, RollbackService

spark = (SparkSession.builder
    .master("local[2]")
    .appName("DataRecoveryLocal")
    .config("spark.sql.warehouse.dir", str(Path("./spark-warehouse").resolve()))
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

for t in ["demo_ingestion", "demo_runbooks", "demo_mttr_occurrence", "cliente_ucs_cliente_quarantine", "demo_cliente_operational"]:
    if spark.catalog.tableExists(t):
        spark.sql(f"DROP TABLE {t}")

now = datetime.now(timezone.utc).replace(tzinfo=None)

# Simula a tabela de ingestão.
ingestion = [
    ("cliente_ucs", "cliente", "ING-001", "FAILED", True, 500, 1000, "Volume abaixo do esperado", now - timedelta(minutes=15)),
    ("outra_base", "cliente", "ING-002", "FAILED", False, 0, 100, "Não elegível", now - timedelta(minutes=10)),
]
cols = ["database_name", "table_name", "ingestion_execution_id", "status", "table_available", "actual_record_count", "expected_record_count", "failure_message", "finished_at"]
spark.createDataFrame(ingestion, cols).write.format("delta").saveAsTable("demo_ingestion")

# Runbooks: primeiro recovery automático, depois rollback, depois fallback manual.
runbooks = [
    ("RBK-010", "FAIL-002", "RB-001", 10, True, "{""database_pattern"":""*_ucs"",""table_pattern"":""cliente"",""max_attempts"":1}", "Recovery automático"),
    ("RBK-011", "FAIL-002", "RB-002", 20, True, "{""database_pattern"":""*_ucs"",""table_pattern"":""*"",""max_attempts"":2}", "Rollback pela quarentena"),
    ("PBK-001", "FAIL-002", "PB-001", 100, True, "{""database_pattern"":""*_ucs""}", "Análise manual"),
]
# Evita YAML/Python adicional: cria a configuração diretamente como strings JSON válidas.
runbook_df = spark.createDataFrame(runbooks, ["code","failure_code","action_code","priority","enabled","conditions_json","description"])
runbook_df = runbook_df.withColumn("conditions", F.from_json(F.col("conditions_json"), "struct<database_pattern:string,table_pattern:string,max_attempts:int>")).drop("conditions_json")
runbook_df.write.format("delta").saveAsTable("demo_runbooks")

# Tabela de quarentena. Neste exemplo o recovery automático sempre tem sucesso;
# ela está disponível para demonstrar o componente de Rollback.
spark.createDataFrame([(1,"João"),(2,"Maria")], ["id","name"]).write.format("delta").saveAsTable("cliente_ucs_cliente_quarantine")
# Tabela operacional placeholder para futura implementação das validações específicas.
spark.createDataFrame([(1,"João"),(2,"Maria")], ["id","name"]).write.format("delta").saveAsTable("demo_cliente_operational")

service = MTTRService(
    ingestion_repo=IngestionRepository(spark, "demo_ingestion"),
    occurrence_repo=OccurrenceRepository(spark, "demo_mttr_occurrence"),
    runbook_service=RunbookService(RunbookRepository(spark, "demo_runbooks")),
    rollback_service=RollbackService(spark, "cliente_ucs_cliente_quarantine"),
    valid_groups={("cliente_ucs", "cliente")},
    lookback_hours=2,
)

print("\n=== EXECUÇÃO 1 ===")
print(service.execute("local-run-001"))

print("\n=== OCORRÊNCIA ===")
spark.table("demo_mttr_occurrence").select("occurrence_id","database_name","table_name","failure_code","runbook_code","action_code","status","attempt_count").show(truncate=False)

print("\n=== ACTIONS ===")
(spark.table("demo_mttr_occurrence")
    .select("occurrence_id", F.explode("actions").alias("action"))
    .select("occurrence_id", "action.event_type", "action.action_code", "action.runbook_code", "action.status", "action.success", "action.attempt", "action.message")
    .show(truncate=False))

print("\n=== EXECUÇÃO 2 (ocorrência existente / idempotência) ===")
print(service.execute("local-run-002"))

spark.stop()
