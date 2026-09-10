from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

@dataclass
class ActionExecution:
    action_id: str
    event_type: str
    action_code: str
    runbook_code: str | None
    status: str
    success: bool
    attempt: int
    started_at: datetime
    finished_at: datetime | None = None
    message: str | None = None
    error_message: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

@dataclass
class Occurrence:
    occurrence_id: str
    occurrence_key: str
    database_name: str
    table_name: str
    failure_code: str
    runbook_code: str | None
    action_code: str | None
    status: str
    first_detected_at: datetime
    last_detected_at: datetime
    attempt_count: int
    execution_id: str
    error_message: str | None
    resolved_at: datetime | None
    actions: list[ActionExecution] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
