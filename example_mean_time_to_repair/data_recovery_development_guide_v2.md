# Data Recovery — Guia de Desenvolvimento

> **Versão:** V1 — Arquitetura simplificada  
> **Objetivo:** servir como contrato de implementação para desenvolvedores e LLMs.  
> **Princípio:** manter qualidade arquitetural sem criar abstrações antes da necessidade.

---

## 1. Objetivo

Criar um módulo de **Data Recovery** executado após o processo de ingestão no Databricks.

O processo de ingestão existente não será alterado.

Fluxo do Job:

```text
Task 1
INGESTÃO
    |
    v
Task 2
MTTR / DATA RECOVERY
    |
    v
Task 3
ROLLBACK
```

O Data Recovery deve:

1. localizar falhas recentes;
2. filtrar falhas elegíveis;
3. consultar o estado da tabela operacional;
4. identificar nova ocorrência ou ocorrência existente;
5. classificar a falha;
6. encontrar um runbook aplicável;
7. selecionar a ação;
8. executar a ação;
9. registrar ações relevantes na própria ocorrência;
10. validar o resultado;
11. atualizar o estado da ocorrência;
12. notificar o grupo responsável quando necessário.

---

# 2. Decisão arquitetural

A solução será um **domínio único de Data Recovery**.

Dentro dele existem dois fluxos:

```text
Data Recovery
|
+-- MTTR
|
+-- Rollback
```

Rollback é uma **capacidade de recuperação** utilizada pelo MTTR.

A Task de Rollback pode permanecer separada no Databricks por decisão operacional, mas não deve duplicar a lógica de decisão do MTTR.

---

# 3. Arquitetura simplificada

A arquitetura V1 possui apenas os componentes necessários:

```text
                MTTRService
                     |
        +------------+------------+
        |            |            |
        v            v            v
   Ingestion      Runbook      Occurrence
   Repository     Service      Repository
                     |
                     v
                  Action
                     |
          +----------+----------+
          |          |          |
       Recovery   Rollback   Playbook/Alert
          |          |          |
          +----------+----------+
                     |
                     v
                 Validation
                     |
                     v
                Occurrence
```

## 3.1 Responsabilidades

```text
MTTRService
    -> coordena o fluxo

RunbookService
    -> decide qual runbook é aplicável

Repositories
    -> acessam dados

Actions
    -> executam ações

RollbackService
    -> realiza restauração pela quarentena

Occurrence
    -> representa o estado e histórico da ocorrência
```

---

# 4. Princípios

## 4.1 Não criar abstrações sem necessidade

Na V1, não criar:

```text
RuleEngine
RuleEvaluator
ActionResolver como classe
StrategyFactory
EventBus
CQRS
Event Sourcing
UnitOfWork
```

Esses componentes podem ser introduzidos futuramente quando houver complexidade real.

## 4.2 Separação mínima obrigatória

Mesmo na arquitetura simplificada, preservar:

```text
decisão != persistência != execução
```

## 4.3 Idempotência

Executar o Job novamente não deve criar ocorrências duplicadas nem repetir uma recuperação já concluída sem necessidade.

## 4.4 Rastreabilidade

Toda ação relevante deve ficar registrada em `mttr_occurrence.actions`.

---

# 5. Estrutura de diretórios

A estrutura oficial da V1 é:

```text
data_recovery/
│
├── notebooks/
│   ├── mttr.py
│   └── rollback.py
│
├── services/
│   ├── mttr_service.py
│   ├── runbook_service.py
│   └── rollback_service.py
│
├── repositories/
│   ├── ingestion_repository.py
│   ├── occurrence_repository.py
│   └── quarantine_repository.py
│
├── actions/
│   ├── recovery.py
│   ├── rollback.py
│   ├── playbook.py
│   └── alert.py
│
├── models/
│   └── occurrence.py
│
├── config/
│   └── runbooks.yml
│
└── tests/
    ├── test_mttr.py
    ├── test_runbook.py
    └── test_rollback.py
```

Não adicionar outros diretórios na V1 sem justificativa técnica.

---

# 6. `notebooks/`

Os notebooks são somente **entry points do Databricks**.

Eles não devem conter regras de negócio relevantes.

---

## 6.1 `notebooks/mttr.py`

### Faz

- receber parâmetros;
- criar repositories;
- criar services;
- executar `MTTRService`;
- retornar resumo.

### Não faz

- classificação complexa;
- seleção de runbook;
- SQL de negócio espalhado;
- implementação de recovery.

Exemplo:

```python
service = build_mttr_service()

result = service.execute(
    execution_id=execution_id
)

dbutils.notebook.exit(str(result))
```

---

## 6.2 `notebooks/rollback.py`

### Faz

- receber ocorrência/tabela a tratar;
- criar dependências;
- chamar `RollbackService`.

### Não faz

- repetir regras do MTTR;
- decidir novamente qual runbook aplicar.

Exemplo:

```python
service = build_rollback_service()

result = service.execute(
    table_name=table_name
)

dbutils.notebook.exit(str(result))
```

---

# 7. `models/`

Contém somente os objetos de negócio usados pelo processo.

---

## 7.1 `models/occurrence.py`

Este é o principal modelo do sistema.

Uma ocorrência representa:

```text
uma falha
+
sua classificação
+
decisão de tratamento
+
estado atual
+
histórico das ações
```

### Modelo

```python
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

    failure_code: str | None
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
```

### Responsabilidade

Representar o estado da ocorrência.

### Não faz

- consultar Delta;
- executar ação;
- enviar Teams.

---

# 8. Contrato da tabela `mttr_occurrence`

A V1 utiliza **uma única tabela principal**:

```text
mttr_occurrence
```

Campos:

| Campo | Tipo | Obrigatório | Regra |
|---|---|---:|---|
| `occurrence_id` | STRING | Sim | identificador único |
| `occurrence_key` | STRING | Sim | chave idempotente |
| `database_name` | STRING | Sim | base afetada |
| `table_name` | STRING | Sim | tabela afetada |
| `failure_code` | STRING | Condicional | obrigatório após classificação |
| `runbook_code` | STRING | Não | preenchido quando aplicável |
| `action_code` | STRING | Não | preenchido quando houver ação |
| `status` | STRING | Sim | estado atual |
| `first_detected_at` | TIMESTAMP | Sim | primeira detecção |
| `last_detected_at` | TIMESTAMP | Sim | última observação |
| `attempt_count` | INT | Sim | contador de tentativas |
| `execution_id` | STRING | Sim | última execução que alterou o registro |
| `error_message` | STRING | Não | último erro |
| `resolved_at` | TIMESTAMP | Condicional | obrigatório se `RESOLVED` |
| `actions` | ARRAY<STRUCT> | Sim | histórico de ações/eventos |
| `created_at` | TIMESTAMP | Sim | criação |
| `updated_at` | TIMESTAMP | Sim | atualização |

---

# 9. Contrato de `actions`

`actions` deve ser armazenado como:

```text
ARRAY<STRUCT>
```

Modelo:

```text
actions ARRAY<
    STRUCT<
        action_id: STRING,
        event_type: STRING,
        action_code: STRING,
        runbook_code: STRING,
        status: STRING,
        success: BOOLEAN,
        attempt: INT,
        started_at: TIMESTAMP,
        finished_at: TIMESTAMP,
        message: STRING,
        error_message: STRING,
        details: MAP<STRING, STRING>
    >
>
```

Campos obrigatórios:

```text
action_id
event_type
action_code
status
success
attempt
started_at
```

Campos condicionais:

```text
runbook_code
finished_at
error_message
message
details
```

---

# 10. Regra de histórico

`actions` é **append-only semanticamente**.

Isso significa:

```text
não apagar ação anterior
não sobrescrever tentativa anterior
não esconder falha passada
```

Exemplo:

```text
actions:
    ACTION_STARTED
    ACTION_FAILED
    ROLLBACK_STARTED
    ROLLBACK_SUCCESS
    OCCURRENCE_RESOLVED
```

Fisicamente o Delta pode regravar a linha, mas o código deve preservar os elementos existentes.

---

# 11. Eventos relevantes

Valores recomendados:

```text
OCCURRENCE_IDENTIFIED
RUNBOOK_SELECTED
ACTION_STARTED
ACTION_SUCCESS
ACTION_FAILED
ROLLBACK_REQUESTED
ROLLBACK_STARTED
ROLLBACK_SUCCESS
ROLLBACK_FAILED
VALIDATION_SUCCESS
VALIDATION_FAILED
OCCURRENCE_RESOLVED
OCCURRENCE_ESCALATED
```

Não é necessário registrar todos os eventos para todas as ocorrências.

Registrar somente os eventos que realmente acontecerem.

---

# 12. `repositories/`

Repositories encapsulam acesso às tabelas.

Regra:

> Repository acessa dados; não toma decisões de negócio.

---

## 12.1 `ingestion_repository.py`

### Faz

Consulta as falhas da ingestão.

Contrato:

```python
get_recent_failures(hours: int) -> list[Failure]
```

A implementação real pode usar:

```python
spark.sql(...)
```

ou DataFrame API.

### Não faz

- classificar falha;
- selecionar runbook;
- executar recovery.

---

## 12.2 `occurrence_repository.py`

### Faz

Persistir e consultar `mttr_occurrence`.

Contratos:

```python
find_by_key(occurrence_key: str) -> Occurrence | None

save(occurrence: Occurrence) -> None

append_action(
    occurrence_id: str,
    action: ActionExecution
) -> None
```

Pode também ter:

```python
get_or_create(...)
```

quando isso simplificar a implementação.

### Não faz

- decidir se runbook é aplicável;
- executar recovery.

---

## 12.3 `quarantine_repository.py`

### Faz

Interagir com a camada de quarentena.

Contratos:

```python
exists(table_name: str) -> bool

restore(table_name: str) -> dict
```

### Não faz

- decidir quando rollback é necessário;
- classificar a falha.

---

# 13. `services/`

Services implementam os casos de uso.

Na V1 existem somente três services.

---

# 14. `services/mttr_service.py`

É o **orquestrador principal**.

Fluxo:

```text
buscar falhas
    ↓
elegibilidade
    ↓
obter/criar ocorrência
    ↓
classificar falha
    ↓
resolver runbook
    ↓
executar action
    ↓
registrar resultado
    ↓
validar
    ↓
atualizar ocorrência
```

### Exemplo

```python
class MTTRService:

    def execute(self, execution_id: str):

        failures = self.ingestion_repository.get_recent_failures(
            hours=self.lookback_hours
        )

        for failure in failures:

            if not self.is_eligible(failure):
                continue

            failure_code = self.classify_failure(failure)

            occurrence = (
                self.occurrence_repository
                .get_or_create(
                    failure=failure,
                    failure_code=failure_code,
                    execution_id=execution_id,
                )
            )

            runbook = self.runbook_service.resolve(
                failure=failure,
                occurrence=occurrence,
                failure_code=failure_code,
            )

            result = self.execute_action(
                runbook,
                occurrence,
            )

            self.update_occurrence(
                occurrence,
                result,
            )
```

---

# 15. Elegibilidade

Na V1, não criar `EligibilityService`.

O `MTTRService` pode possuir um método:

```python
def is_eligible(self, failure) -> bool:
    ...
```

Regras iniciais:

```text
database contém "_ucs"
grupo válido existe
```

Outras regras simples podem ser adicionadas aqui.

Quando a quantidade de regras justificar, extrair posteriormente.

---

# 16. Classificação da falha

Na V1, a classificação também pode permanecer no `MTTRService`.

Método:

```python
def classify_failure(self, failure) -> str:
    ...
```

Exemplo:

```python
if failure.table_available is False:
    return "FAIL-001"

if (
    failure.actual_record_count is not None
    and failure.expected_record_count is not None
    and failure.actual_record_count
    < failure.expected_record_count
):
    return "FAIL-002"

if failure.ingestion_status == "FAILED":
    return "FAIL-003"

return "FAIL-999"
```

Não executar ações dentro do classifier.

---

# 17. `services/runbook_service.py`

É responsável por **encontrar e selecionar o runbook aplicável**.

Este serviço responde:

> “Dado o contexto da ocorrência, qual runbook pode ser utilizado?”

Fluxo:

```text
failure_code
    ↓
buscar candidatos
    ↓
avaliar condições
    ↓
filtrar candidatos
    ↓
ordenar por prioridade
    ↓
selecionar runbook
```

Exemplo:

```python
class RunbookService:

    def resolve(
        self,
        failure,
        occurrence,
        failure_code,
    ):

        candidates = self.repository.find_candidates(
            failure_code=failure_code,
            database_name=failure.database_name,
            table_name=failure.table_name,
        )

        applicable = [
            runbook
            for runbook in candidates
            if self.matches(
                runbook,
                failure,
                occurrence,
            )
        ]

        if not applicable:
            return self.default_runbook()

        return min(
            applicable,
            key=lambda item: item["priority"],
        )
```

---

# 18. Avaliação das condições do Runbook

Na V1, não criar `RuleEvaluator`.

Usar método interno:

```python
def matches(self, runbook, failure, occurrence):
    ...
```

Exemplo:

```python
def matches(self, runbook, failure, occurrence):

    conditions = runbook["conditions"]

    max_attempts = conditions.get("max_attempts")

    if max_attempts is not None:
        if occurrence.attempt_count >= max_attempts:
            return False

    return True
```

Se esse método ficar grande, criar `rule_evaluator.py` numa versão futura.

---

# 19. `services/rollback_service.py`

Implementa o caso de uso de Rollback.

Fluxo:

```text
validar tabela
    ↓
validar dados
    ↓
rollback necessário?
    |
    +-- não -> finalizar
    |
    +-- sim
          ↓
      quarentena existe?
          |
          +-- não -> alertar
          |
          +-- sim
                ↓
             restore
                ↓
             validar
```

Exemplo:

```python
class RollbackService:

    def execute(self, table_name: str):

        if not self.operational_repository.exists(table_name):
            return {
                "success": False,
                "status": "TABLE_UNAVAILABLE",
            }

        state = self.operational_repository.get_state(
            table_name
        )

        if self.is_valid(state):
            return {
                "success": True,
                "status": "NOT_REQUIRED",
            }

        if not self.quarantine_repository.exists(
            table_name
        ):
            return {
                "success": False,
                "status": "NO_QUARANTINE",
            }

        restore = self.quarantine_repository.restore(
            table_name
        )

        if not restore["success"]:
            return restore

        return self.validate_restore(
            table_name
        )
```

---

# 20. `actions/`

Actions executam operações concretas.

Não decidem se a ação é aplicável.

A decisão acontece antes, no `RunbookService`.

---

# 21. `actions/recovery.py`

Executa uma recuperação técnica automática.

Exemplo:

```python
def execute(occurrence):

    # Implementação técnica da recuperação

    return {
        "success": True,
        "status": "SUCCESS",
        "message": "Recovery executado",
    }
```

---

# 22. `actions/rollback.py`

É o adaptador da ação de Rollback.

```python
def execute(occurrence, rollback_service):

    return rollback_service.execute(
        occurrence.table_name
    )
```

Não duplicar a lógica presente em `RollbackService`.

---

# 23. `actions/playbook.py`

Utilizado quando a solução requer ação manual.

Pode:

- informar runbook;
- enviar instrução;
- orientar análise;
- registrar escalonamento.

Não deve executar recuperação automática.

---

# 24. `actions/alert.py`

Utilizado para comunicação operacional.

Exemplo:

```python
def execute(occurrence, teams_client):

    teams_client.send(
        occurrence=occurrence,
        message="Falha sem tratamento automático",
    )

    return {
        "success": False,
        "status": "ESCALATED",
    }
```

---

# 25. Registry de Actions

Na V1, um registry simples é suficiente.

Exemplo:

```python
ACTIONS = {
    "RB-001": recovery.execute,
    "RB-002": rollback.execute,
    "PB-001": playbook.execute,
    "OC-001": alert.execute,
}
```

Execução:

```python
action = ACTIONS[runbook.action_code]

result = action(
    occurrence
)
```

Não criar `ActionResolver` como classe enquanto esse registry for suficiente.

---

# 26. Relação entre Runbook e Action

Não confundir:

```text
Failure Code
Runbook Code
Action Code
```

Exemplo:

```text
FAIL-002
    |
    +-- RBK-010 -> RB-001
    |
    +-- RBK-011 -> RB-002
    |
    +-- PBK-001 -> PB-001
```

Significados:

```text
FAIL-002
    = o que aconteceu

RBK-010
    = qual procedimento se aplica

RB-001
    = como tratar
```

---

# 27. Configuração `config/runbooks.yml`

Exemplo:

```yaml
runbooks:

  - code: RBK-010
    failure_code: FAIL-002
    action_code: RB-001
    priority: 10
    enabled: true

    conditions:
      database_pattern: "*_ucs"
      table_pattern: "cliente"
      max_attempts: 1

    description: "Recovery automático da tabela cliente"

  - code: RBK-011
    failure_code: FAIL-002
    action_code: RB-002
    priority: 20
    enabled: true

    conditions:
      database_pattern: "*_ucs"
      table_pattern: "*"
      max_attempts: 2

    description: "Rollback utilizando quarentena"

  - code: PBK-001
    failure_code: FAIL-002
    action_code: PB-001
    priority: 100
    enabled: true

    conditions:
      database_pattern: "*_ucs"

    description: "Análise manual"
```

---

# 28. Fluxo de decisão do Runbook

O comportamento deve ser:

```text
Failure
  ↓
Failure Code
  ↓
RunbookService
  ↓
buscar candidatos
  ↓
avaliar condições
  ↓
apenas aplicáveis
  ↓
menor priority
  ↓
Runbook
  ↓
Action Code
```

Se nenhum runbook for aplicável:

```text
NO_APPLICABLE_RUNBOOK
        ↓
OC-001
        ↓
ESCALATED
```

---

# 29. Fluxo de uma ocorrência

Exemplo:

```text
database = cliente_ucs
table = cliente
ingestion = FAILED
```

## Etapa 1

```text
is_eligible()
    -> true
```

## Etapa 2

```text
classify_failure()
    -> FAIL-002
```

## Etapa 3

```text
get_or_create()
    -> INC-001
```

## Etapa 4

```text
RunbookService
    -> RBK-010
```

## Etapa 5

```text
RBK-010
    -> RB-001
```

## Etapa 6

```text
ACTIONS["RB-001"]
    -> recovery.execute()
```

## Etapa 7

```text
validation
    -> success
```

## Etapa 8

```text
status
    -> RESOLVED
```

E `actions` fica:

```text
[
  RUNBOOK_SELECTED,
  ACTION_STARTED,
  ACTION_SUCCESS,
  VALIDATION_SUCCESS,
  OCCURRENCE_RESOLVED
]
```

---

# 30. Fluxo com falha do Recovery

```text
FAIL-002
   ↓
RBK-010
   ↓
RB-001
   ↓
Recovery
   ↓
FAILED
   ↓
RB-002
   ↓
Rollback
   ↓
SUCCESS
   ↓
RESOLVED
```

Nesse cenário a ocorrência registra:

```text
ACTION_STARTED    RB-001
ACTION_FAILED     RB-001
ROLLBACK_STARTED  RB-002
ROLLBACK_SUCCESS  RB-002
OCCURRENCE_RESOLVED
```

Isso permite acompanhar todo o processo sem outra tabela de histórico.

---

# 31. Rollback como parte da recuperação

Rollback não deve possuir seu próprio mecanismo de decisão de negócio independente do MTTR.

A relação deve ser:

```text
MTTR
  ↓
decide rollback
  ↓
RB-002
  ↓
RollbackService
  ↓
QuarantineRepository
```

Se Rollback for executado como Task separada:

```text
MTTR
  ↓
status = ROLLBACK_REQUIRED
  ↓
Task Rollback
  ↓
RollbackService
```

A Task não deve reclassificar toda a ocorrência.

---

# 32. Registro de ações relevantes

Toda ação relevante deve ser registrada em `Occurrence.actions`.

Regra:

```text
Antes da execução:
ACTION_STARTED

Após sucesso:
ACTION_SUCCESS

Após falha:
ACTION_FAILED
```

Para rollback:

```text
ROLLBACK_REQUESTED
ROLLBACK_STARTED
ROLLBACK_SUCCESS
```

ou:

```text
ROLLBACK_REQUESTED
ROLLBACK_STARTED
ROLLBACK_FAILED
```

---

# 33. Atualização da ocorrência

Exemplo de estado:

```text
status = PROCESSING
```

durante execução.

Depois:

```text
RESOLVED
```

ou:

```text
ESCALATED
```

Campos relevantes:

```text
attempt_count
last_detected_at
updated_at
error_message
resolved_at
```

---

# 34. Idempotência

A `occurrence_key` deve ser determinística.

Recomendação:

```text
database_name + table_name + failure_code
```

Exemplo:

```python
raw = (
    f"{database_name}|"
    f"{table_name}|"
    f"{failure_code}"
)
```

Pode-se utilizar SHA-256 como valor físico.

---

# 35. Controle de processamento

Antes de executar uma ação automática:

```text
DETECTED
    ↓
PROCESSING
```

A transição deve ocorrer de forma condicional.

Se outra execução já tiver colocado a ocorrência em `PROCESSING`, a execução atual não deve executar novamente a mesma ação.

---

# 36. Validação

Toda recuperação automática deve ser validada.

Exemplos:

```text
tabela existe?
dados existem?
quantidade correta?
última carga atualizada?
```

O resultado deve ser:

```text
VALID
```

ou:

```text
INVALID
```

A validação deve ocorrer depois de:

```text
Recovery
```

ou:

```text
Rollback
```

---

# 37. Critérios para `RESOLVED`

Uma ocorrência só deve ser marcada como:

```text
RESOLVED
```

quando:

1. a ação terminou;
2. a validação foi executada;
3. a validação confirmou o resultado esperado.

Além disso:

```text
resolved_at != NULL
```

---

# 38. Critérios para `ESCALATED`

Usar `ESCALATED` quando:

```text
nenhum runbook aplicável
```

ou:

```text
ação automática falhou
```

ou:

```text
não existe quarentena para rollback
```

ou:

```text
validação pós-recuperação falhou
```

A ocorrência deve possuir informação suficiente em:

```text
error_message
actions
```

---

# 39. Testes

## `tests/test_mttr.py`

Testar:

```text
falha elegível
falha não elegível
ocorrência nova
ocorrência existente
idempotência
recovery sucesso
recovery falha
escalonamento
```

## `tests/test_runbook.py`

Testar:

```text
runbook aplicável
runbook não aplicável
prioridade
runbook desabilitado
fallback
```

## `tests/test_rollback.py`

Testar:

```text
tabela válida
rollback não necessário
quarentena inexistente
restore sucesso
restore falha
validação pós-restore
```

---

# 40. O que deve permanecer simples

Na V1:

```text
MTTRService
    |
    +-- regras simples
    +-- RunbookService
    +-- repositories
    +-- actions
```

Não criar uma cadeia como:

```text
MTTRService
 -> EligibilityService
 -> Classifier
 -> RuleEngine
 -> RuleEvaluator
 -> RunbookResolver
 -> ActionResolver
 -> StrategyFactory
 -> Strategy
 -> EventBus
```

Esse desenho só deve aparecer se a complexidade futura justificar.

---

# 41. Quando extrair novos módulos

## Extrair `FailureClassifier`

Quando classificação ocupar uma parte grande do `MTTRService`.

## Extrair `RuleEvaluator`

Quando `RunbookService.matches()` possuir muitas regras compostas.

## Criar `ActionResolver`

Quando a quantidade de actions tornar o registry difícil de manter.

## Criar Strategy Pattern

Quando as actions tiverem comportamentos complexos e compartilharem abstrações.

---

# 42. Checklist de implementação

Antes de considerar uma alteração concluída:

```text
[ ] Não alterou ingestão
[ ] Está no diretório correto
[ ] Não colocou regra de negócio no notebook
[ ] Acesso a tabela está no repository
[ ] Runbook é decidido pelo RunbookService
[ ] Action executa, mas não decide
[ ] Action relevante foi registrada em actions
[ ] Ocorrência continua idempotente
[ ] Resultado foi validado
[ ] Status atualizado
[ ] Teste foi criado/ajustado
```

---

# 43. Regras obrigatórias para LLM

Ao gerar código neste projeto:

1. Ler este documento antes da implementação.
2. Preferir a solução mais simples que satisfaça o contrato.
3. Não criar novas abstrações sem necessidade.
4. Não modificar ingestão.
5. Não mover regras entre camadas sem justificativa.
6. Não criar tabelas adicionais de histórico sem decisão explícita.
7. Manter `actions` como histórico append-only semântico.
8. Manter idempotência.
9. Adicionar testes para comportamento novo.
10. Não duplicar lógica de Rollback.

---

# 44. Contrato resumido

A regra central da arquitetura é:

```text
MTTRService
    |
    |-- "essa falha entra?"
    |
    |-- "o que aconteceu?"
    |
    v
RunbookService
    |
    |-- "qual procedimento se aplica?"
    |
    v
Action
    |
    |-- "execute"
    |
    v
Validation
    |
    |-- "funcionou?"
    |
    v
Occurrence
    |
    |-- "registre o estado e as actions"
```

---

# 45. Definição final

O sistema deve ser simples o suficiente para ser entendido por uma única pessoa, mas estruturado o suficiente para permitir evolução.

A arquitetura oficial da V1 é:

```text
                +------------------+
                |   MTTRService    |
                +---------+--------+
                          |
             +------------+------------+
             |                         |
             v                         v
       Repositories              RunbookService
                                     |
                                     v
                                  Action
                           +---------+---------+
                           |         |         |
                       Recovery   Rollback  Playbook/Alert
                           |         |         |
                           +---------+---------+
                                     |
                                     v
                                  Validate
                                     |
                                     v
                              mttr_occurrence
                                     |
                                     v
                              actions ARRAY<STRUCT>
```

> **Regra principal:** detectar, decidir, executar, validar e registrar — com o mínimo de abstrações necessárias.
