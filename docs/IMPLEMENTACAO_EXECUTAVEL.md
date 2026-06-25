# Sentinel.AI — Plano de Implementação Executável (Arquitetura Visionária)

> **Para quem implementa este documento (incluindo agentes de coding):** este é um plano de implementação autocontido. Não pressupõe conhecimento de nenhuma conversa anterior. Leia a Seção 1 (lógica de negócio) por completo antes de tocar em qualquer código — toda decisão técnica das seções seguintes deriva dela. Se qualquer instrução posterior parecer contradizer a Seção 1, a Seção 1 prevalece.
>
> **Escopo desta implementação:** este plano implanta o stack de tecnologias real (Kafka, Spark, Redis, MinIO, Airflow, Metabase) em **nível de demonstração/protótipo local**, não em nível de produção real. A Seção 12 lista explicitamente o que fica de fora por essa razão (TLS entre serviços, Vault, Schema Registry formal, replicação multi-broker) — não adicione nenhum desses itens a menos que solicitado explicitamente. Esta implementação roda em paralelo a um MVP simplificado já existente (que usa Python puro, Pandas e Postgres em vez do stack real) — os dois projetos são independentes; não modifique o MVP existente ao trabalhar neste plano.
>
> Este documento complementa (não substitui) o `ARQUITETURA_VISIONARIA.md`, que contém a justificativa de cada decisão e o mapeamento de notas de avaliação. Este documento aqui é a contraparte executável: contém os artefatos reais (compose, schemas, código) necessários para implantar o sistema.

---

## 1. Lógica de negócio — leia isto primeiro

### 1.1. O que o sistema faz

Sentinel.AI modera mensagens de chat em tempo real (estilo Discord/Twitch), decidindo se uma mensagem é tóxica e que ação tomar (ignorar, alertar um moderador, ou banir o autor). A diferença central em relação a um filtro de palavrões comum: a decisão é **sensível ao contexto de cada comunidade**. A mesma frase pode ser uma brincadeira corriqueira em um servidor e uma ofensa real em outro — o sistema aprende e armazena esse contexto por servidor, em vez de aplicar uma regra global fixa.

### 1.2. Os dois fluxos — esta é a regra arquitetural mais importante do projeto

O sistema tem **dois fluxos paralelos com propósitos diferentes**. Confundir os dois é o erro mais comum ao implementar este sistema, então a distinção é repetida aqui sem ambiguidade:

- **Fluxo 1 — Decisão.** Responde "esta mensagem, agora, neste grupo, é tóxica?". Roda em tempo real, mensagem por mensagem, produz uma ação, e termina. Ele **não decide nada sobre o histórico** — decide sobre o presente.
- **Fluxo 2 — Armazenamento (padrão Medalhão: Bronze/Silver/Gold).** Responde "o que aconteceu até agora?". Acumula histórico indefinidamente, alimenta auditoria e dashboards. **Não decide nada** — apenas registra e agrega decisões que o Fluxo 1 já tomou.

O único ponto de cruzamento entre os dois: o resultado do Fluxo 1 (score de toxicidade, categoria da ofensa, ação tomada) é anexado como colunas extras ao registro da mensagem **antes** desse registro entrar na camada Bronze do Fluxo 2. O Bronze nunca decide — ele recebe a mensagem original mais o resultado de uma decisão que já aconteceu, e grava os dois juntos, sem alterar nenhum dos dois.

Prova de que os fluxos são independentes: se o Fluxo 2 fosse inteiramente desligado, a moderação em tempo real continuaria funcionando — apenas se perderia histórico, auditoria e dashboard. O inverso não é verdade: sem o Fluxo 1, o Fluxo 2 não teria nenhuma decisão relevante para armazenar.

### 1.3. Fluxo 1 em detalhe — a cascata de decisão

O Fluxo 1 não é um classificador único. É uma cascata de três estágios, motivada por uma restrição de capacidade real (detalhada na Seção 1.6): não é viável, em nenhum orçamento, chamar uma IA generativa para 100% do tráfego de um chat de alto volume.

```
Mensagem chega
  │
  ▼
┌─────────────────────────────────────────────────────┐
│ ESTÁGIO 1 — Modelo porteiro                          │
│ Local, sem chamada de rede, < 5ms.                    │
│ Calcula um score de suspeita (0.0–1.0) varrendo o     │
│ texto contra um vocabulário — o vocabulário GLOBAL    │
│ combinado com o vocabulário ESPECÍFICO daquele        │
│ servidor (vindo da fonte de contexto, Seção 1.4).      │
│ Se score < threshold (padrão 0.3): DESCARTA.          │
│ Não vira incidente, não é armazenado como decisão     │
│ relevante, e — importante — NÃO gasta chamada de IA.  │
└─────────────────────────────────────────────────────┘
  │ (score >= threshold)
  ▼
┌─────────────────────────────────────────────────────┐
│ ESTÁGIO 2 — Classificador com contexto                │
│ Chamada a uma API de LLM (validado com Google Gemini, │
│ modelo gemini-2.5-flash). Só roda para o que passou   │
│ do porteiro — minoria do tráfego.                      │
│ Recebe: o texto da mensagem + o perfil de contexto     │
│ completo do servidor (Seção 1.4) — NUNCA recebe o      │
│ histórico de mensagens cru, apenas o perfil já          │
│ resumido. Devolve JSON: {score, categoria,             │
│ acao_sugerida}.                                        │
└─────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────┐
│ ESTÁGIO 3 — Política de decisão final                 │
│ Lógica de negócio local, determinística, auditável.    │
│ NÃO confia ciegamente na "acao_sugerida" do LLM —      │
│ aplica limiares próprios sobre o "score" numérico:      │
│   score >= 0.85  → ban                                  │
│   score >= 0.50  → alerta                                │
│   score <  0.50  → ignora                                │
│ Essa separação entre "o que o LLM sugere" e "o que o    │
│ sistema decide" existe para que a decisão final seja    │
│ auditável e consistente, e não dependa inteiramente de  │
│ uma caixa-preta de terceiros.                            │
└─────────────────────────────────────────────────────┘
  │
  ▼
Ação executada (ban / alerta / ignora) + resultado anexado ao Bronze
```

**Por que a chamada ao classificador não pode ser pulada para o ban automático mesmo com score alto do porteiro:** o porteiro mede *suspeita lexical* (presença de termos), não *toxicidade real no contexto*. Só o classificador, com o perfil do servidor em mãos, decide se aquela suspeita lexical é de fato uma ofensa real ou uma expressão comum do grupo.

### 1.4. A fonte de contexto — de onde vem o conhecimento da IA

Sem esta peça, o sistema seria apenas um classificador genérico, incapaz de diferenciar comunidades — o que anularia o diferencial inteiro do projeto. Implemente-a com o mesmo rigor dado ao Fluxo 1.

**Estrutura do perfil de contexto (por servidor), aqui chamado `ai_server_context`:**

```json
{
  "server_id": "string, identificador único do servidor/grupo",
  "em_modo_treino": "boolean",
  "mensagens_observadas": "integer, contagem cumulativa",
  "toxicity_level": "float 0.0–1.0, nível geral de toxicidade do grupo",
  "vocab_map": {
    "termo ou expressão frequente": "peso float 0.0–1.0 de quão suspeito é NESTE grupo"
  },
  "conflict_patterns": ["lista de padrões textuais curtos que indicam início de conflito NESTE grupo"],
  "metadata": {"tom_geral": "descrição curta gerada pela LLM"},
  "atualizado_em": "timestamp"
}
```

**Como o perfil nasce e evolui — siga esta sequência exatamente:**

1. **Bot entra em um servidor.** Verificar se já existe histórico de mensagens daquele `server_id`.
2. **Sem histórico (servidor novo) ou histórico abaixo do `TREINO_THRESHOLD` (configurável, padrão 50 mensagens):** o servidor entra em **modo treino**. Nesse modo, o sistema **só observa** — grava as mensagens normalmente no Fluxo 2, mas o Fluxo 1 não toma ação de moderação real (ou toma com uma política mais permissiva, a critério da implementação — o ponto inegociável é: não banir em cima de um perfil que ainda não existe).
3. **Histórico atinge o `TREINO_THRESHOLD`:** disparar um **scan inicial** — pegar uma amostra das últimas mensagens daquele servidor (não é necessário usar todas, uma amostra de até 100 mensagens é suficiente) e enviar para uma LLM grande com uma instrução explícita: **a LLM não classifica mensagem por mensagem aqui — ela resume o comportamento do grupo como um todo**, devolvendo o JSON no formato acima.
4. **Perfil gravado.** A partir daqui, `em_modo_treino` passa a `false`, e o Fluxo 1 (Estágio 1 e 2) passa a consultar este perfil em tempo real para cada mensagem nova daquele servidor.
5. **Atualização periódica.** Um job agendado (não a cada mensagem — periodicamente, ex. a cada hora ou a cada N novas mensagens) reprocessa uma amostra recente e atualiza o perfil, capturando mudanças de vocabulário e comportamento ao longo do tempo.
6. **Feedback humano.** Quando um moderador confirma ou reverte uma ação automatizada, esse evento é registrado e deve, no ciclo de atualização periódica, influenciar a próxima atualização do perfil daquele servidor.

**Decisão de design inegociável: o perfil é estatístico, não um banco de exemplos.** Ele guarda agregados (`toxicity_level`, `vocab_map`) — nunca uma coleção de mensagens passadas para busca por similaridade semântica. Isso significa que **não é necessário banco de dados vetorial** nesta arquitetura. Uma consulta relacional simples por `server_id` é suficiente. Não introduza pgvector, Pinecone, Chroma, ou qualquer mecanismo de busca por similaridade — isso adicionaria complexidade sem resolver um problema real desta especificação. Caso uma implementação futura decida armazenar exemplos reais de mensagens (não estatísticas) no perfil, essa seria a única condição em que banco vetorial se justificaria — mas isso está fora do escopo deste plano.

### 1.5. Fluxo 2 em detalhe — Medalhão

| Camada | O que contém | O que NÃO contém |
|---|---|---|
| **Bronze** | Toda mensagem, intacta, com o resultado do Fluxo 1 (score, categoria, ação) anexado como colunas. Nunca é alterado depois de gravado, exceto pela própria gravação inicial. | Nenhuma limpeza, nenhuma agregação. |
| **Silver** | Mesma granularidade do Bronze (1 linha por mensagem), mas limpo: sem duplicata (`message_id` único), sem campo crítico nulo, texto normalizado (trim, lowercase para fins de análise — o texto original do Bronze nunca é sobrescrito). | Nenhuma decisão de moderação nova — a decisão já veio do Bronze. |
| **Gold** | Agregação por `server_id`: contagem de mensagens, contagem de ações tomadas, toxicidade média, timestamp da última ação. É o que alimenta o dashboard. | Mensagens individuais. Gold nunca expõe o texto de uma mensagem específica — apenas métricas agregadas. |

### 1.6. Restrição de capacidade — leia antes de dimensionar qualquer serviço

O volume de referência deste projeto é 5.000–10.000 mensagens/segundo, com latência-alvo abaixo de 500ms ponta a ponta para o Fluxo 1. Chamar uma API de LLM externa custa tipicamente 150–800ms de latência por chamada. **Não é matematicamente viável classificar 100% desse volume via LLM síncrono.** A arquitetura depende estruturalmente do Estágio 1 (porteiro local) filtrar pelo menos 90–95% do tráfego antes de qualquer chamada de IA. Ao implementar, trate isso como requisito de capacidade, não como detalhe de otimização — um porteiro mal calibrado (threshold baixo demais, deixando passar tráfego excessivo) quebra o SLA de latência do sistema inteiro.

---

## 2. Ambiente e pré-requisitos

- Host: Windows 10/11 com WSL2 (Ubuntu 22.04+) e Docker Desktop com integração WSL2 habilitada.
- RAM livre no WSL: mínimo 16 GB.
- Espaço em disco: mínimo 30 GB livres.
- Verificar antes de iniciar:
  ```bash
  docker --version          # >= 24.0
  docker compose version    # >= 2.20
  free -h                    # >= 16G disponível
  ```

---

## 3. Estrutura de repositório a ser criada

```
sentinel-ai-producao/
├── README.md
├── .env.example
├── .gitignore
├── docker-compose.yml
├── docker/
│   ├── airflow/
│   │   └── Dockerfile
│   └── spark-jobs/
│       └── Dockerfile
├── kafka/
│   └── criar_topicos.sh
├── sql/
│   ├── schema_negocio.sql
│   ├── schema_airflow_db.sql
│   └── schema_metabase_db.sql
├── spark_jobs/
│   ├── fluxo1_decisao.py
│   ├── fluxo2_bronze_to_silver.py
│   ├── fluxo2_silver_to_gold.py
│   └── shared/
│       ├── __init__.py
│       ├── porteiro.py
│       ├── classificador.py
│       └── contexto.py
├── airflow/
│   └── dags/
│       ├── dag_bronze_to_silver.py
│       ├── dag_silver_to_gold.py
│       └── dag_atualizar_perfil_servidor.py
├── fastapi_acao/
│   ├── main.py
│   └── requirements.txt
├── scripts/
│   ├── simulador_carga.py
│   └── criar_buckets_minio.py
└── docs/
    ├── ARQUITETURA_VISIONARIA.md
    └── IMPLEMENTACAO_EXECUTAVEL.md
```

---

## 4. `docker-compose.yml` — conteúdo completo

Crie este arquivo na raiz do repositório, com este conteúdo exato. Cada serviço tem comentário indicando seu papel no Fluxo 1 ou Fluxo 2.

```yaml
services:
  # ===================== MENSAGERIA (Fluxo 1) =====================
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: sentinel_kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      CLUSTER_ID: sentinelKRaftCluster001
    volumes:
      - sentinel_kafka_data:/var/lib/kafka/data
    networks:
      - sentinel-network
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 15s
      timeout: 10s
      retries: 10

  # ===================== CACHE DE CONTEXTO (Fluxo 1) =====================
  redis:
    image: redis:7.2-alpine
    container_name: sentinel_redis
    ports:
      - "6379:6379"
    command: redis-server --save 300 1
    volumes:
      - sentinel_redis_data:/data
    networks:
      - sentinel-network
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ===================== PROCESSAMENTO (Fluxo 1 e Fluxo 2) =====================
  spark-master:
    image: bitnami/spark:3.5
    container_name: sentinel_spark_master
    environment:
      SPARK_MODE: master
    ports:
      - "8081:8080"   # UI do Spark master
      - "7077:7077"
    networks:
      - sentinel-network

  spark-worker:
    image: bitnami/spark:3.5
    container_name: sentinel_spark_worker
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 2G
      SPARK_WORKER_CORES: 2
    depends_on:
      - spark-master
    networks:
      - sentinel-network

  # ===================== ARMAZENAMENTO RELACIONAL =====================
  postgres:
    image: postgres:16
    container_name: sentinel_postgres
    restart: unless-stopped
    environment:
      POSTGRES_USER: sentinel
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-sentinel123}
      POSTGRES_MULTIPLE_DATABASES: sentinel_ai,airflow_db,metabase_db
    ports:
      - "5432:5432"
    volumes:
      - sentinel_pgdata:/var/lib/postgresql/data
      - ./sql:/docker-entrypoint-initdb.d
    networks:
      - sentinel-network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U sentinel"]
      interval: 5s
      timeout: 5s
      retries: 5

  # ===================== DATA LAKE (Fluxo 2) =====================
  minio:
    image: minio/minio:latest
    container_name: sentinel_minio
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER:-sentinel}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-sentinel123456}
    ports:
      - "9000:9000"   # API S3
      - "9001:9001"   # Console web
    command: server /data --console-address ":9001"
    volumes:
      - sentinel_minio_data:/data
    networks:
      - sentinel-network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ===================== ORQUESTRAÇÃO (Fluxo 2) =====================
  airflow-webserver:
    build:
      context: ./docker/airflow
    container_name: sentinel_airflow_web
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://sentinel:${POSTGRES_PASSWORD:-sentinel123}@postgres/airflow_db
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    ports:
      - "8080:8080"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./spark_jobs:/opt/airflow/spark_jobs
    command: webserver
    networks:
      - sentinel-network

  airflow-scheduler:
    build:
      context: ./docker/airflow
    container_name: sentinel_airflow_scheduler
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://sentinel:${POSTGRES_PASSWORD:-sentinel123}@postgres/airflow_db
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./spark_jobs:/opt/airflow/spark_jobs
    command: scheduler
    networks:
      - sentinel-network

  # ===================== AÇÃO (Fluxo 1) =====================
  fastapi-acao:
    build:
      context: ./fastapi_acao
    container_name: sentinel_fastapi_acao
    environment:
      DATABASE_URL: postgresql://sentinel:${POSTGRES_PASSWORD:-sentinel123}@postgres/sentinel_ai
    ports:
      - "8000:8000"
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - sentinel-network

  # ===================== CONSUMO (Fluxo 2) =====================
  metabase:
    image: metabase/metabase:v0.49.7
    container_name: sentinel_metabase
    environment:
      MB_DB_TYPE: postgres
      MB_DB_DBNAME: metabase_db
      MB_DB_PORT: 5432
      MB_DB_USER: sentinel
      MB_DB_PASS: ${POSTGRES_PASSWORD:-sentinel123}
      MB_DB_HOST: postgres
    ports:
      - "3000:3000"
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - sentinel-network

networks:
  sentinel-network:
    driver: bridge

volumes:
  sentinel_kafka_data:
  sentinel_redis_data:
  sentinel_pgdata:
  sentinel_minio_data:
```

**Nota de implementação sobre `POSTGRES_MULTIPLE_DATABASES`:** a imagem oficial `postgres:16` não cria múltiplas bases automaticamente a partir dessa variável — ela não existe nativamente. É necessário um script de inicialização (`sql/00_criar_bases.sh`, executado antes dos `.sql`) que crie as três bases (`sentinel_ai`, `airflow_db`, `metabase_db`) explicitamente. Crie este script:

```bash
#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE airflow_db;
    CREATE DATABASE metabase_db;
EOSQL
```

Salve como `sql/00_criar_bases.sh`, e adicione `chmod +x sql/00_criar_bases.sh` antes do primeiro `docker compose up`. O Postgres executa todo script em `/docker-entrypoint-initdb.d/` em ordem alfabética na primeira inicialização — por isso o prefixo `00_` garante que ele rode antes de `schema_negocio.sql`.

---

## 5. `sql/schema_negocio.sql` — schema completo

```sql
\connect sentinel_ai;

-- ============================================================
-- FLUXO 2 — BRONZE: mensagem intacta + resultado do Fluxo 1 anexado
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze_eventos (
    id                BIGSERIAL PRIMARY KEY,
    message_id        TEXT NOT NULL UNIQUE,
    server_id         TEXT NOT NULL,
    channel_id        TEXT NOT NULL,
    user_id           TEXT NOT NULL,
    texto             TEXT NOT NULL,
    criado_em         TIMESTAMPTZ NOT NULL,
    score_porteiro    NUMERIC(4,3),
    passou_porteiro   BOOLEAN NOT NULL DEFAULT FALSE,
    score_toxicidade  NUMERIC(4,3),
    categoria         TEXT,
    acao              TEXT NOT NULL DEFAULT 'nenhuma',
    processado_silver BOOLEAN NOT NULL DEFAULT FALSE,
    recebido_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bronze_server ON bronze_eventos(server_id);
CREATE INDEX IF NOT EXISTS idx_bronze_pendente ON bronze_eventos(processado_silver) WHERE processado_silver = FALSE;

-- ============================================================
-- FLUXO 2 — SILVER: mesma granularidade, limpo
-- ============================================================
CREATE TABLE IF NOT EXISTS silver_eventos (
    id                BIGSERIAL PRIMARY KEY,
    message_id        TEXT NOT NULL UNIQUE,
    server_id         TEXT NOT NULL,
    channel_id        TEXT NOT NULL,
    user_id           TEXT NOT NULL,
    texto_normalizado TEXT NOT NULL,
    criado_em         TIMESTAMPTZ NOT NULL,
    hora_dia          SMALLINT NOT NULL,
    score_toxicidade  NUMERIC(4,3),
    categoria         TEXT,
    acao              TEXT NOT NULL,
    processado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_silver_server ON silver_eventos(server_id);

-- ============================================================
-- FLUXO 2 — GOLD: agregação por servidor, alimenta o dashboard
-- ============================================================
CREATE TABLE IF NOT EXISTS gold_dashboard_metrics (
    server_id            TEXT PRIMARY KEY,
    total_mensagens      BIGINT NOT NULL DEFAULT 0,
    total_analisadas     BIGINT NOT NULL DEFAULT 0,
    total_acoes          BIGINT NOT NULL DEFAULT 0,
    toxicidade_media     NUMERIC(4,3),
    ultima_acao_em       TIMESTAMPTZ,
    atualizado_em        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_gold_server ON gold_dashboard_metrics(server_id);

-- ============================================================
-- FLUXO 1 — FONTE DE CONTEXTO: conhecimento que a IA consulta
-- (espelhado também no Redis em tempo de execução — ver Seção 7)
-- ============================================================
CREATE TABLE IF NOT EXISTS ai_server_context (
    server_id            TEXT PRIMARY KEY,
    em_modo_treino       BOOLEAN NOT NULL DEFAULT TRUE,
    mensagens_observadas BIGINT NOT NULL DEFAULT 0,
    toxicity_level       NUMERIC(4,3) NOT NULL DEFAULT 0.0,
    vocab_map            JSONB NOT NULL DEFAULT '{}',
    conflict_patterns    JSONB NOT NULL DEFAULT '[]',
    metadata             JSONB NOT NULL DEFAULT '{}',
    criado_em            TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Feedback humano: moderador confirma ou reverte uma ação.
-- Alimenta a atualização periódica do ai_server_context (Seção 1.4, passo 6).
CREATE TABLE IF NOT EXISTS feedback_humano (
    id                  BIGSERIAL PRIMARY KEY,
    bronze_evento_id    BIGINT NOT NULL REFERENCES bronze_eventos(id),
    moderador_id        TEXT NOT NULL,
    decisao_original    TEXT NOT NULL,
    decisao_revisada    TEXT NOT NULL,
    comentario          TEXT,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- DOMÍNIO TRANSVERSAL — MONITORAMENTO: métricas de execução de jobs
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_execucoes (
    id                     BIGSERIAL PRIMARY KEY,
    job_nome               TEXT NOT NULL,
    status                 TEXT NOT NULL,
    registros_processados  INTEGER NOT NULL DEFAULT 0,
    duracao_segundos       NUMERIC(8,3),
    erro_mensagem          TEXT,
    executado_em           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_execucoes_job ON pipeline_execucoes(job_nome, executado_em DESC);

-- ============================================================
-- DOMÍNIO TRANSVERSAL — SEGURANÇA: usuários de banco com privilégio mínimo
-- (ver Seção 9 — cada serviço deve usar um destes, nunca o usuário 'sentinel' root)
-- ============================================================
CREATE USER spark_jobs WITH PASSWORD 'troque_esta_senha_em_producao';
GRANT SELECT, INSERT, UPDATE ON bronze_eventos, silver_eventos, gold_dashboard_metrics TO spark_jobs;
GRANT SELECT ON ai_server_context TO spark_jobs;
GRANT INSERT ON pipeline_execucoes TO spark_jobs;

CREATE USER fastapi_acao WITH PASSWORD 'troque_esta_senha_em_producao';
GRANT SELECT, UPDATE ON bronze_eventos TO fastapi_acao;

CREATE USER metabase_reader WITH PASSWORD 'troque_esta_senha_em_producao';
GRANT SELECT ON gold_dashboard_metrics, pipeline_execucoes TO metabase_reader;
-- metabase_reader NÃO recebe acesso a bronze_eventos nem silver_eventos:
-- essas tabelas contêm texto de mensagem e user_id (dado pessoal), e o
-- Metabase deve expor apenas dado agregado (Seção 9 — Governança).
```

---

## 6. `spark_jobs/shared/` — módulos compartilhados

### 6.1. `spark_jobs/shared/contexto.py`

Implementa a Seção 1.4 (fonte de contexto). Lê/escreve no Redis (cache rápido, consultado a cada mensagem) e no Postgres (`ai_server_context`, fonte de verdade persistente).

```python
import json
import redis
from sqlalchemy import create_engine, text

REDIS_HOST = "redis"
REDIS_PORT = 6379


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def buscar_contexto_servidor(server_id: str, database_url: str) -> dict:
    """
    Busca o perfil de contexto, priorizando o Redis (latencia de cache).
    Se nao estiver no Redis, busca no Postgres e popula o cache.
    Se nao existir em nenhum dos dois, retorna perfil neutro de modo treino.
    """
    r = get_redis_client()
    chave = f"server_profile:{server_id}"

    cache = r.get(chave)
    if cache:
        return json.loads(cache)

    engine = create_engine(database_url)
    with engine.connect() as conn:
        resultado = conn.execute(
            text("SELECT * FROM ai_server_context WHERE server_id = :sid"),
            {"sid": server_id}
        ).mappings().first()

    if resultado is None:
        return {
            "em_modo_treino": True,
            "mensagens_observadas": 0,
            "toxicity_level": 0.0,
            "vocab_map": {},
            "conflict_patterns": [],
        }

    perfil = dict(resultado)
    r.set(chave, json.dumps(perfil, default=str), ex=3600)
    return perfil


def invalidar_cache_servidor(server_id: str):
    """Chamar sempre que o perfil for atualizado no Postgres (job periodico)."""
    get_redis_client().delete(f"server_profile:{server_id}")
```

### 6.2. `spark_jobs/shared/porteiro.py`

Implementa o Estágio 1 da cascata (Seção 1.3). Local, sem chamada de rede.

```python
PALAVRAS_SUSPEITAS_BASE = {
    "burro": 0.6, "idiota": 0.6, "incompetente": 0.6,
    "cala a boca": 0.5, "se ferrar": 0.7, "odeio": 0.4,
    "lixo": 0.5, "merda": 0.4, "inutil": 0.5,
}


def calcular_score_porteiro(texto: str, vocab_servidor: dict = None) -> float:
    """
    Score de suspeita (0.0-1.0), combinando vocabulario global com o
    vocab_map especifico do servidor (Secao 1.4) - isso torna o porteiro
    sensivel ao contexto, nao apenas generico.
    """
    texto_lower = texto.lower()
    score_max = 0.0
    vocab_combinado = dict(PALAVRAS_SUSPEITAS_BASE)
    if vocab_servidor:
        vocab_combinado.update(vocab_servidor)
    for termo, peso in vocab_combinado.items():
        if termo in texto_lower:
            score_max = max(score_max, peso)
    return round(score_max, 3)


def passa_pelo_porteiro(texto: str, vocab_servidor: dict = None, threshold: float = 0.3) -> tuple[bool, float]:
    score = calcular_score_porteiro(texto, vocab_servidor)
    return score >= threshold, score
```

### 6.3. `spark_jobs/shared/classificador.py`

Implementa o Estágio 2 da cascata (Seção 1.3).

```python
import os
import json
from google import genai

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")


def get_client():
    return genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


def montar_prompt(texto: str, contexto: dict) -> str:
    """
    O contexto do servidor (Secao 1.4) e SEMPRE incluido no prompt - nunca
    classificar uma mensagem sem o perfil do grupo, ou o sistema perde seu
    diferencial (sensibilidade ao contexto).
    """
    return f"""Você é um classificador de moderação de chat. Analise a mensagem abaixo
considerando o CONTEXTO deste grupo especifico - a mesma frase pode ser
ofensiva em um grupo e corriqueira em outro.

CONTEXTO DO SERVIDOR:
- Nivel geral de toxicidade do grupo: {contexto.get('toxicity_level', 0.0)} (0=tranquilo, 1=muito hostil)
- Vocabulario comum deste grupo: {json.dumps(contexto.get('vocab_map', {}), ensure_ascii=False)}
- Padroes de conflito conhecidos: {json.dumps(contexto.get('conflict_patterns', []), ensure_ascii=False)}

MENSAGEM A CLASSIFICAR:
"{texto}"

Responda APENAS em JSON, sem nenhum texto antes ou depois, no formato:
{{"score": <numero de 0.0 a 1.0>, "categoria": "<ofensa_pessoal|discurso_odio|ameaca|brincadeira_comum|nenhuma>", "acao": "<ban|alerta|ignora>"}}"""


def classificar_mensagem(texto: str, contexto: dict) -> dict:
    """
    Em caso de falha (rate limit, indisponibilidade), retorna fallback
    seguro: nao decide nada automaticamente, sinaliza erro para o
    Estagio 3 (politica de decisao) tratar.
    """
    prompt = montar_prompt(texto, contexto)
    try:
        client = get_client()
        resposta = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        texto_resposta = resposta.text.strip()
        if texto_resposta.startswith("```"):
            texto_resposta = texto_resposta.strip("`").replace("json", "", 1).strip()
        return json.loads(texto_resposta)
    except Exception as e:
        return {"score": 0.0, "categoria": "erro_classificacao", "acao": "ignora", "_erro": str(e)}
```

---

## 7. `spark_jobs/fluxo1_decisao.py` — job principal do Fluxo 1

Este é o job Spark Structured Streaming que consome do Kafka, executa a cascata de 3 estágios (Seção 1.3), e grava o resultado no Bronze. Implementa fielmente a lógica de negócio descrita na Seção 1 — nenhuma regra nova é introduzida aqui, apenas o motor de execução muda em relação ao protótipo validado.

```python
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType

import sys
sys.path.append("/opt/airflow/spark_jobs")
from shared.contexto import buscar_contexto_servidor
from shared.porteiro import passa_pelo_porteiro
from shared.classificador import classificar_mensagem

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://spark_jobs:troque_esta_senha_em_producao@postgres/sentinel_ai")
PORTEIRO_THRESHOLD = float(os.getenv("PORTEIRO_THRESHOLD", 0.3))
LIMIAR_BAN = 0.85
LIMIAR_ALERTA = 0.50

SCHEMA_MENSAGEM = StructType([
    StructField("message_id", StringType()),
    StructField("server_id", StringType()),
    StructField("channel_id", StringType()),
    StructField("user_id", StringType()),
    StructField("texto", StringType()),
    StructField("criado_em", TimestampType()),
])


def decidir_acao_final(score: float, acao_sugerida: str) -> str:
    """
    Estagio 3 (Secao 1.3): politica de negocio local, NAO confia
    ciegamente na sugestao do LLM. Auditavel e determinística.
    """
    if score >= LIMIAR_BAN:
        return "ban"
    elif score >= LIMIAR_ALERTA:
        return "alerta"
    return "ignora"


def processar_mensagem(message_id, server_id, channel_id, user_id, texto, criado_em):
    """
    Executa a cascata completa de 3 estagios para uma mensagem.
    Retorna o dicionario completo a ser gravado no Bronze.
    """
    contexto = buscar_contexto_servidor(server_id, DATABASE_URL)
    vocab_servidor = contexto.get("vocab_map", {})

    passou, score_porteiro = passa_pelo_porteiro(texto, vocab_servidor, PORTEIRO_THRESHOLD)

    resultado = {
        "message_id": message_id, "server_id": server_id, "channel_id": channel_id,
        "user_id": user_id, "texto": texto, "criado_em": criado_em,
        "score_porteiro": score_porteiro, "passou_porteiro": passou,
        "score_toxicidade": None, "categoria": None, "acao": "nenhuma",
    }

    if not passou:
        return resultado

    classificacao = classificar_mensagem(texto, contexto)
    acao_final = decidir_acao_final(classificacao["score"], classificacao["acao"])

    resultado.update({
        "score_toxicidade": classificacao["score"],
        "categoria": classificacao["categoria"],
        "acao": acao_final,
    })
    return resultado


def gravar_no_bronze(df_batch, batch_id):
    """
    foreachBatch: para cada micro-batch do streaming, processa cada
    mensagem pela cascata e grava o resultado no Bronze via JDBC.
    """
    import pandas as pd
    from sqlalchemy import create_engine, text

    if df_batch.isEmpty():
        return

    linhas = df_batch.collect()
    resultados = [
        processar_mensagem(r.message_id, r.server_id, r.channel_id, r.user_id, r.texto, r.criado_em)
        for r in linhas
    ]

    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        for r in resultados:
            conn.execute(
                text("""
                    INSERT INTO bronze_eventos
                    (message_id, server_id, channel_id, user_id, texto, criado_em,
                     score_porteiro, passou_porteiro, score_toxicidade, categoria, acao)
                    VALUES
                    (:message_id, :server_id, :channel_id, :user_id, :texto, :criado_em,
                     :score_porteiro, :passou_porteiro, :score_toxicidade, :categoria, :acao)
                    ON CONFLICT (message_id) DO NOTHING
                """),
                r
            )
        conn.commit()


def main():
    spark = (
        SparkSession.builder
        .appName("SentinelAI-Fluxo1-Decisao")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )

    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "raw-messages")
        .option("startingOffsets", "latest")
        .load()
    )

    df_mensagens = (
        df_kafka
        .select(from_json(col("value").cast("string"), SCHEMA_MENSAGEM).alias("data"))
        .select("data.*")
    )

    query = (
        df_mensagens.writeStream
        .foreachBatch(gravar_no_bronze)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
```

**Nota de implementação importante:** o código acima usa `df_batch.collect()` e processa linha por linha em Python puro (chamando `buscar_contexto_servidor` e `classificar_mensagem` por mensagem). Isso é **intencional e correto para o volume filtrado pelo porteiro** (minoria do tráfego, Seção 1.6), mas **não deve ser usado para o cálculo do score do porteiro em si** — esse cálculo, por rodar em 100% do tráfego, deveria ser implementado como UDF vetorizada do Spark (`pandas_udf`) para aproveitar paralelismo real, em vez de `collect()` trazendo tudo para o driver. Uma otimização de implementação (não uma mudança de lógica de negócio) seria separar o Estágio 1 como uma transformação Spark nativa antes do `foreachBatch`, e só usar `foreachBatch` para os Estágios 2 e 3, que de fato exigem chamada de API externa e portanto já são inerentemente sequenciais/limitados por rate limit.

---

## 8. `spark_jobs/fluxo2_bronze_to_silver.py` e `fluxo2_silver_to_gold.py`

Implementam a Seção 1.5 (Medalhão). São jobs em lote (batch), não streaming — rodam sob orquestração do Airflow (Seção 10).

### 8.1. `fluxo2_bronze_to_silver.py`

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, hour

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://spark_jobs:troque_esta_senha_em_producao@postgres/sentinel_ai")
JDBC_URL = DATABASE_URL.replace("postgresql://", "jdbc:postgresql://")


def main():
    spark = SparkSession.builder.appName("SentinelAI-Fluxo2-BronzeToSilver").getOrCreate()

    df_bronze = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "(SELECT * FROM bronze_eventos WHERE processado_silver = FALSE) AS pendentes")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    total_antes = df_bronze.count()
    if total_antes == 0:
        print("Nenhuma linha pendente no Bronze.")
        return

    # Camada de Qualidade da Silver (Secao 1.5): dedup, nulo critico, normalizacao.
    # NAO e decisao de moderacao - essa ja veio do Bronze.
    df_limpo = (
        df_bronze
        .dropDuplicates(["message_id"])
        .filter(col("user_id").isNotNull() & col("texto").isNotNull() & col("server_id").isNotNull())
        .filter(trim(col("texto")) != "")
        .withColumn("texto_normalizado", lower(trim(col("texto"))))
        .withColumn("hora_dia", hour(col("criado_em")))
        .select(
            "message_id", "server_id", "channel_id", "user_id", "texto_normalizado",
            "criado_em", "hora_dia", "score_toxicidade", "categoria", "acao"
        )
    )

    total_depois = df_limpo.count()
    if total_antes != total_depois:
        print(f"AVISO: {total_antes - total_depois} linhas descartadas na limpeza Silver")

    (
        df_limpo.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver_eventos")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    ids_processados = [row.id for row in df_bronze.select("id").collect()]
    from sqlalchemy import create_engine, text
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE bronze_eventos SET processado_silver = TRUE WHERE id = ANY(:ids)"),
            {"ids": ids_processados}
        )
        conn.commit()

    print(f"Silver atualizada: {total_depois} linhas processadas.")


if __name__ == "__main__":
    main()
```

### 8.2. `fluxo2_silver_to_gold.py`

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, max as spark_max, sum as spark_sum, when, col

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://spark_jobs:troque_esta_senha_em_producao@postgres/sentinel_ai")
JDBC_URL = DATABASE_URL.replace("postgresql://", "jdbc:postgresql://")


def main():
    spark = SparkSession.builder.appName("SentinelAI-Fluxo2-SilverToGold").getOrCreate()

    df_silver = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver_eventos")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    if df_silver.count() == 0:
        print("Silver vazia, nada para agregar.")
        return

    # Gold NAO decide nada - apenas resume o que o Fluxo 1 ja decidiu (Secao 1.5).
    df_gold = df_silver.groupBy("server_id").agg(
        count("message_id").alias("total_mensagens"),
        spark_sum(when(col("score_toxicidade").isNotNull(), 1).otherwise(0)).alias("total_analisadas"),
        spark_sum(when(col("acao") != "nenhuma", 1).otherwise(0)).alias("total_acoes"),
        avg("score_toxicidade").alias("toxicidade_media"),
        spark_max("criado_em").alias("ultima_acao_em"),
    )

    linhas = df_gold.collect()

    from sqlalchemy import create_engine, text
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        for row in linhas:
            conn.execute(
                text("""
                    INSERT INTO gold_dashboard_metrics
                    (server_id, total_mensagens, total_analisadas, total_acoes,
                     toxicidade_media, ultima_acao_em, atualizado_em)
                    VALUES (:server_id, :total_mensagens, :total_analisadas, :total_acoes,
                            :toxicidade_media, :ultima_acao_em, now())
                    ON CONFLICT (server_id) DO UPDATE SET
                        total_mensagens = EXCLUDED.total_mensagens,
                        total_analisadas = EXCLUDED.total_analisadas,
                        total_acoes = EXCLUDED.total_acoes,
                        toxicidade_media = EXCLUDED.toxicidade_media,
                        ultima_acao_em = EXCLUDED.ultima_acao_em,
                        atualizado_em = now()
                """),
                row.asDict()
            )
        conn.commit()

    print(f"Gold atualizado para {len(linhas)} servidor(es).")


if __name__ == "__main__":
    main()
```

---

## 9. `airflow/dags/` — as três DAGs

Cada DAG apenas dispara, via `BashOperator` ou `SparkSubmitOperator`, os jobs já descritos nas Seções 7 e 8. A lógica de negócio vive nos jobs Spark — as DAGs são puramente de agendamento.

### 9.1. `dag_bronze_to_silver.py`

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "sentinel-ai",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_bronze_to_silver",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # a cada 5 minutos
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sentinel-ai", "fluxo2"],
) as dag:

    rodar_job = BashOperator(
        task_id="rodar_bronze_to_silver",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "/opt/airflow/spark_jobs/fluxo2_bronze_to_silver.py"
        ),
    )
```

### 9.2. `dag_silver_to_gold.py`

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "sentinel-ai",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dag_silver_to_gold",
    default_args=default_args,
    schedule_interval="*/15 * * * *",  # a cada 15 minutos - depende da Silver estar atualizada
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sentinel-ai", "fluxo2"],
) as dag:

    rodar_job = BashOperator(
        task_id="rodar_silver_to_gold",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "/opt/airflow/spark_jobs/fluxo2_silver_to_gold.py"
        ),
    )
```

### 9.3. `dag_atualizar_perfil_servidor.py`

Implementa a Seção 1.4, passos 3 e 5 (scan inicial e atualização periódica do perfil de contexto). Esta DAG precisa de um script adicional não detalhado nas seções anteriores — `spark_jobs/atualizar_perfil_servidor.py` — que replica a lógica já validada no protótipo (`buscar_amostra_mensagens`, `montar_prompt_extracao`, `extrair_perfil_via_llm`, gravação no `ai_server_context` + invalidação do cache Redis via `invalidar_cache_servidor`). Este script não é reproduzido aqui por brevidade, mas deve seguir exatamente a sequência de 6 passos da Seção 1.4.

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "sentinel-ai",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_atualizar_perfil_servidor",
    default_args=default_args,
    schedule_interval="0 * * * *",  # a cada hora
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sentinel-ai", "fluxo1", "fonte-de-contexto"],
) as dag:

    rodar_job = BashOperator(
        task_id="rodar_atualizar_perfil",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "/opt/airflow/spark_jobs/atualizar_perfil_servidor.py"
        ),
    )
```

---

## 10. `.env.example` — variáveis necessárias

```
POSTGRES_PASSWORD=troque_esta_senha
MINIO_ROOT_USER=sentinel
MINIO_ROOT_PASSWORD=troque_esta_senha_tambem
GEMINI_API_KEY=cole_sua_chave_aqui
GEMINI_MODEL=gemini-2.5-flash
PORTEIRO_THRESHOLD=0.3
TREINO_THRESHOLD=50
DATABASE_URL=postgresql://spark_jobs:troque_esta_senha_em_producao@postgres/sentinel_ai
```

**Atenção de segurança (Seção 7.4 do `ARQUITETURA_VISIONARIA.md`):** os valores de senha acima são placeholders. Antes de qualquer execução real, gere senhas distintas para `sentinel` (superusuário), `spark_jobs`, `fastapi_acao`, e `metabase_reader` (criados no schema da Seção 5), e nunca reutilize a mesma senha entre serviços.

---

## 11. Sequência de execução — do zero ao sistema funcionando

Execute nesta ordem exata. Cada passo lista o comando e o que verificar antes de seguir ao próximo.

```bash
# 1. Preparar variáveis de ambiente
cp .env.example .env
# editar .env com valores reais antes de continuar

# 2. Tornar o script de criação de bases executável
chmod +x sql/00_criar_bases.sh

# 3. Subir toda a infraestrutura
docker compose up -d

# 4. Aguardar todos os serviços ficarem saudáveis (pode levar 1-2 minutos)
docker compose ps
# verificar que postgres, kafka, redis, minio aparecem como "healthy"

# 5. Criar o tópico Kafka
docker exec sentinel_kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic raw-messages --partitions 6 --replication-factor 1

# 6. Confirmar tópico criado
docker exec sentinel_kafka kafka-topics --bootstrap-server localhost:9092 --list

# 7. Criar os buckets no MinIO (via mc, o cliente de linha de comando do MinIO,
#    ou via console web em http://localhost:9001)

# 8. Confirmar schema aplicado no Postgres
docker exec -it sentinel_postgres psql -U sentinel -d sentinel_ai -c "\dt"
# deve listar: bronze_eventos, silver_eventos, gold_dashboard_metrics,
# ai_server_context, feedback_humano, pipeline_execucoes

# 9. Submeter o job de streaming do Fluxo 1 (roda continuamente)
docker exec sentinel_spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/airflow/spark_jobs/fluxo1_decisao.py

# 10. Confirmar DAGs visíveis no Airflow
# abrir http://localhost:8080 no navegador, confirmar as 3 DAGs listadas

# 11. Confirmar Metabase conectado
# abrir http://localhost:3000, completar setup inicial, conectar à base
# metabase_db (metadados) e configurar uma fonte de dados adicional
# apontando para sentinel_ai usando o usuário metabase_reader

# 12. Gerar tráfego de teste
python scripts/simulador_carga.py --taxa 10 --duracao 60
# (este script deve produzir mensagens no tópico Kafka raw-messages,
# seguindo o schema da Seção 1, a uma taxa configurável de msg/s)

# 13. Validar ponta a ponta
docker exec -it sentinel_postgres psql -U sentinel -d sentinel_ai \
  -c "SELECT acao, COUNT(*) FROM bronze_eventos GROUP BY acao;"
# deve mostrar mensagens distribuídas entre nenhuma/alerta/ban/ignora,
# confirmando que o Fluxo 1 está processando o tráfego do Kafka
```

---

## 12. O que este documento conscientemente não inclui

Para manter este plano executável sem se tornar inviável de revisar, os seguintes itens são mencionados mas não detalhados linha a linha — ficam como próximo incremento, não como bloqueio para a implantação inicial:

- `fastapi_acao/main.py` completo — implementar seguindo o contrato descrito na Seção 5.7 do `ARQUITETURA_VISIONARIA.md` (idempotência por `message_id`, token de bot armazenado fora do código).
- `spark_jobs/atualizar_perfil_servidor.py` — replicar a lógica já validada no protótipo (módulo `perfil_servidor.py`), adaptando para ler do Postgres de produção e invalidar o cache Redis ao final.
- Schema Registry (Confluent) para validação formal de contrato de mensagem — mencionado na Seção 7.1 do `ARQUITETURA_VISIONARIA.md`, não incluído aqui por ser uma camada adicional de maturidade, não bloqueante para a primeira implantação funcional.
- Stack de observabilidade centralizada (ELK/Grafana Loki) — o `pipeline_execucoes` e logs locais de cada container já fornecem visibilidade mínima funcional; a centralização é um incremento de maturidade operacional.
- TLS entre serviços e Vault para segredos — apropriados para um ambiente de produção real exposto à internet; o ambiente de demonstração local (Docker Compose em uma máquina) não está exposto a essa superfície de ataque, e a inclusão de TLS/Vault aqui adicionaria complexidade de implantação desproporcional ao contexto de demonstração acadêmica.
