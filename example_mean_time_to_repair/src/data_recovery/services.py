from __future__ import annotations
from datetime import datetime, timezone
from fnmatch import fnmatch
from hashlib import sha256
from .models import Occurrence, ActionExecution
from . import actions


def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)

class RunbookService:
    def __init__(self, repository):
        self.repository = repository

    def resolve(self, failure: dict, occurrence: Occurrence, failure_code: str):
        for rb in self.repository.find_candidates(failure_code):
            c = rb.get("conditions") or {}
            if c.get("database_pattern") and not fnmatch(failure["database_name"], c["database_pattern"]):
                continue
            if c.get("table_pattern") and not fnmatch(failure["table_name"], c["table_pattern"]):
                continue
            if c.get("max_attempts") is not None and occurrence.attempt_count >= int(c["max_attempts"]):
                continue
            return rb
        return {"code": None, "action_code": "OC-001", "priority": 9999, "description": "Nenhum runbook aplicável"}

class RollbackService:
    def __init__(self, spark, quarantine_table: str):
        self.spark = spark
        self.quarantine_table = quarantine_table

    def execute(self, table_name: str):
        if not self.spark.catalog.tableExists(table_name):
            return {"success": False, "status": "TABLE_UNAVAILABLE", "message": f"Tabela indisponível: {table_name}", "details": {}}
        if not self.spark.catalog.tableExists(self.quarantine_table):
            return {"success": False, "status": "NO_QUARANTINE", "message": "Quarentena não encontrada", "details": {}}
        try:
            # Demo: restaura a versão inteira da quarentena para a tabela.
            q = self.spark.table(self.quarantine_table)
            q.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
            count = self.spark.table(table_name).count()
            return {"success": True, "status": "SUCCESS", "message": f"Rollback concluído: {count} registros", "details": {"restored_records": str(count)}}
        except Exception as exc:
            return {"success": False, "status": "RESTORE_FAILED", "message": str(exc), "details": {}}

class MTTRService:
    def __init__(self, ingestion_repo, occurrence_repo, runbook_service, rollback_service, valid_groups, lookback_hours=2):
        self.ingestion_repo = ingestion_repo
        self.occurrence_repo = occurrence_repo
        self.runbook_service = runbook_service
        self.rollback_service = rollback_service
        self.valid_groups = valid_groups
        self.lookback_hours = lookback_hours

    def execute(self, execution_id: str):
        failures = self.ingestion_repo.get_recent_failures(self.lookback_hours)
        summary = {"total_failures": len(failures), "eligible": 0, "new": 0, "existing": 0, "resolved": 0, "escalated": 0}
        for failure in failures:
            if not self._eligible(failure):
                continue
            summary["eligible"] += 1
            failure_code = self._classify(failure)
            occurrence, is_new = self._get_or_create(failure, failure_code, execution_id)
            summary["new" if is_new else "existing"] += 1

            # Idempotência da V1: uma ocorrência já resolvida não deve ser
            # processada novamente pela mesma chave de ocorrência.
            if not is_new and occurrence.status == "RESOLVED":
                continue

            runbook = self.runbook_service.resolve(failure, occurrence, failure_code)
            occurrence.runbook_code = runbook.get("code")
            occurrence.action_code = runbook.get("action_code")
            self._event(occurrence, "RUNBOOK_SELECTED", occurrence.action_code or "OC-001", occurrence.runbook_code, "SELECTED", True, runbook.get("description", "Runbook selecionado"))
            action_code = occurrence.action_code or "OC-001"
            if action_code == "RB-001":
                fn = lambda: actions.recovery(occurrence)
            elif action_code == "RB-002":
                fn = lambda: actions.rollback(occurrence, self.rollback_service)
            elif action_code == "PB-001":
                fn = lambda: actions.playbook(occurrence, runbook)
            else:
                fn = lambda: actions.alert(occurrence)
            self._event(occurrence, "ACTION_STARTED", action_code, occurrence.runbook_code, "PROCESSING", True, "Ação iniciada")
            result = fn()
            ok = bool(result["success"])
            self._event(occurrence, "ACTION_SUCCESS" if ok else "ACTION_FAILED", action_code, occurrence.runbook_code, result["status"], ok, result["message"], None if ok else result["message"], result.get("details"))
            if ok:
                occurrence.status = "RESOLVED"
                occurrence.resolved_at = now_utc()
                self._event(occurrence, "OCCURRENCE_RESOLVED", action_code, occurrence.runbook_code, "RESOLVED", True, "Ocorrência resolvida")
                summary["resolved"] += 1
            else:
                occurrence.status = "ESCALATED"
                occurrence.error_message = result["message"]
                summary["escalated"] += 1
            self._persist(occurrence)
        return summary

    def _eligible(self, f):
        return "_ucs" in f["database_name"] and (f["database_name"], f["table_name"]) in self.valid_groups

    @staticmethod
    def _classify(f):
        if f.get("table_available") is False:
            return "FAIL-001"
        if f.get("actual_record_count") is not None and f.get("expected_record_count") is not None and f["actual_record_count"] < f["expected_record_count"]:
            return "FAIL-002"
        if f.get("ingestion_status") == "FAILED":
            return "FAIL-003"
        return "FAIL-999"

    def _get_or_create(self, f, failure_code, execution_id):
        raw = f'{f["database_name"]}|{f["table_name"]}|{failure_code}'
        key = sha256(raw.encode()).hexdigest()
        row = self.occurrence_repo.find_by_key(key)
        if row:
            occ = Occurrence(**row)
            occ.execution_id = execution_id
            occ.last_detected_at = f["finished_at"]
            return occ, False
        t = now_utc()
        occ = Occurrence(
            occurrence_id=f"INC-{t.strftime('%Y%m%d%H%M%S%f')}",
            occurrence_key=key,
            database_name=f["database_name"],
            table_name=f["table_name"],
            failure_code=failure_code,
            runbook_code=None,
            action_code=None,
            status="DETECTED",
            first_detected_at=f["finished_at"],
            last_detected_at=f["finished_at"],
            attempt_count=0,
            execution_id=execution_id,
            error_message=f.get("failure_message"),
            resolved_at=None,
            created_at=t,
            updated_at=t,
        )
        self._persist(occ)
        self._event(occ, "OCCURRENCE_IDENTIFIED", "SYSTEM", None, "RECORDED", True, "Ocorrência identificada")
        return occ, True

    def _event(self, occ, event_type, action_code, runbook_code, status, success, message, error_message=None, details=None):
        t = now_utc()
        if event_type == "ACTION_STARTED":
            occ.attempt_count += 1
        action = ActionExecution(
            action_id=f"{occ.occurrence_id}-{len(occ.actions)+1:03d}",
            event_type=event_type,
            action_code=action_code,
            runbook_code=runbook_code,
            status=status,
            success=success,
            attempt=max(1, occ.attempt_count),
            started_at=t,
            finished_at=t,
            message=message,
            error_message=error_message,
            details={k: str(v) for k, v in (details or {}).items()},
        )
        occ.actions.append(action)
        occ.updated_at = t
        self._persist(occ)

    def _persist(self, occ):
        self.occurrence_repo.save({
            "occurrence_id": occ.occurrence_id,
            "occurrence_key": occ.occurrence_key,
            "database_name": occ.database_name,
            "table_name": occ.table_name,
            "failure_code": occ.failure_code,
            "runbook_code": occ.runbook_code,
            "action_code": occ.action_code,
            "status": occ.status,
            "first_detected_at": occ.first_detected_at,
            "last_detected_at": occ.last_detected_at,
            "attempt_count": occ.attempt_count,
            "execution_id": occ.execution_id,
            "error_message": occ.error_message,
            "resolved_at": occ.resolved_at,
            "actions": [a.__dict__ for a in occ.actions],
            "created_at": occ.created_at,
            "updated_at": occ.updated_at,
        })
