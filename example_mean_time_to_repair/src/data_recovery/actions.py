from typing import Any

def recovery(occurrence) -> dict[str, Any]:
    # Simulação da ação técnica. Substituir pela ação real.
    return {"success": True, "status": "SUCCESS", "message": "Recovery técnico concluído", "details": {"mode": "demo"}}

def rollback(occurrence, rollback_service) -> dict[str, Any]:
    return rollback_service.execute(occurrence.table_name)

def playbook(occurrence, runbook) -> dict[str, Any]:
    return {"success": True, "status": "ESCALATED", "message": "Atuação manual necessária", "details": {"documentation_url": str(runbook.get("documentation_url") or "")}}

def alert(occurrence) -> dict[str, Any]:
    return {"success": False, "status": "ESCALATED", "message": "Falha sem recuperação automática", "details": {}}
