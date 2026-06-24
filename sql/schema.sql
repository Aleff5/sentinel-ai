-- BRONZE: cada mensagem original com o resultado da decisão de moderação anexado 
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

-- SILVER:  sem duplicata, sem campo nulo crítico, texto padronizado.
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

-- GOLD: agregação por servidor, 
CREATE TABLE IF NOT EXISTS gold_dashboard_metrics (
    server_id            TEXT PRIMARY KEY,
    total_mensagens      BIGINT NOT NULL DEFAULT 0,
    total_analisadas     BIGINT NOT NULL DEFAULT 0,
    total_acoes          BIGINT NOT NULL DEFAULT 0,
    toxicidade_media     NUMERIC(4,3),
    ultima_acao_em       TIMESTAMPTZ,
    atualizado_em        TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- AI_SERVER_CONTEXT: o conhecimento do classificador. resumo estatístico do comportamento daquele servidor.
CREATE TABLE IF NOT EXISTS ai_server_context (
    server_id          TEXT PRIMARY KEY,
    em_modo_treino      BOOLEAN NOT NULL DEFAULT TRUE,
    mensagens_observadas BIGINT NOT NULL DEFAULT 0,

    toxicity_level      NUMERIC(4,3) NOT NULL DEFAULT 0.0,
    vocab_map            JSONB NOT NULL DEFAULT '{}',
    conflict_patterns    JSONB NOT NULL DEFAULT '[]',
    metadata             JSONB NOT NULL DEFAULT '{}',

    criado_em            TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Feedback humano: moderador confirma ou reverte uma ação.
CREATE TABLE IF NOT EXISTS feedback_humano (
    id                  BIGSERIAL PRIMARY KEY,
    bronze_evento_id    BIGINT NOT NULL REFERENCES bronze_eventos(id),
    moderador_id        TEXT NOT NULL,
    decisao_original    TEXT NOT NULL,
    decisao_revisada    TEXT NOT NULL,
    comentario           TEXT,
    criado_em            TIMESTAMPTZ NOT NULL DEFAULT now()
);


--MONITORAMENTO: métricas de execução de cada job do pipeline
CREATE TABLE IF NOT EXISTS pipeline_execucoes (
    id                BIGSERIAL PRIMARY KEY,
    job_nome          TEXT NOT NULL,
    status            TEXT NOT NULL,
    registros_processados INTEGER NOT NULL DEFAULT 0,
    duracao_segundos  NUMERIC(8,3),
    erro_mensagem     TEXT,
    executado_em      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_execucoes_job ON pipeline_execucoes(job_nome, executado_em DESC);