# Data Recovery — Exemplo local com Spark

Exemplo funcional de referência baseado na arquitetura V1 simplificada.

## Fluxo

```text
IngestionRepository
      |
      v
MTTRService
      |
      +--> eligibility
      +--> occurrence
      +--> classification
      +--> RunbookService
      +--> Action
      +--> Validation/result
      |
      v
mttr_occurrence
      |
      +--> actions ARRAY<STRUCT>
```

## Arquitetura da V1

```text
MTTRService
   |
   +-- Repositories
   +-- RunbookService
   +-- Actions
   +-- RollbackService
   +-- Occurrence
```

Não há `RuleEngine`, `RuleEvaluator`, `ActionResolver`, `StrategyFactory` ou outras abstrações desnecessárias para a V1.

## Pré-requisitos

- Python 3.10+
- Java compatível com PySpark 3.5.x
- PySpark 3.5.x
- Delta Lake

Instalação:

```bash
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell
# .venv\\Scripts\\Activate.ps1

pip install -r requirements.txt
```

Execução:

```bash
PYTHONPATH=src python run_local.py
```

No Windows PowerShell:

```powershell
$env:PYTHONPATH="src"
python run_local.py
```

## O que o exemplo demonstra

1. Cria uma tabela simulando o controle da ingestão.
2. Consulta falhas das últimas 2 horas.
3. Filtra somente `*_ucs` com grupo válido.
4. Classifica `FAIL-002` para volume abaixo do esperado.
5. Busca runbooks candidatos.
6. Seleciona o runbook de maior prioridade aplicável.
7. Executa `RB-001` como recovery automático.
8. Registra eventos em `actions ARRAY<STRUCT>`.
9. Marca a ocorrência como `RESOLVED`.
10. Executa uma segunda vez para demonstrar ocorrência existente/idempotência.

## Observação importante sobre produção

O `recovery` e o `rollback` neste projeto são demonstrações. Em produção, substituir a lógica técnica pelos procedimentos reais.

Também é necessário adaptar:

- nomes das tabelas existentes;
- regra de contagem esperada;
- validação pós-carga;
- regras de quarentena;
- notificação Teams;
- controle de concorrência;
- credenciais e configuração Databricks.
