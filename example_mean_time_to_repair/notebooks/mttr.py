# Databricks entry point (exemplo)
# Em produção, monte os adapters Spark/Delta e injete-os no MTTRService.
from data_recovery.services import MTTRService

# service = build_service_from_databricks()
# result = service.execute(dbutils.widgets.get("execution_id"))
# dbutils.notebook.exit(str(result))
