import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import streamlit as st
import pandas as pd
from sqlalchemy import text
from src.db import get_engine
from src.ingestion.simulator import simular_lote
from src.decision.engine import processar_lote_pendente
from src.pipeline.silver import processar_silver
from src.pipeline.gold import processar_gold

st.set_page_config(page_title="Sentinel.AI", page_icon="🛡️", layout="wide")

st.title("Sentinel.AI — Painel de Moderação")
st.caption("Pipeline de moderação de conteúdo em tempo real com IA contextual")

aba_simular, aba_dashboard, aba_perfis, aba_logs = st.tabs(
    ["Simular mensagem", "Dashboard", "Perfis de servidor", "🩺 Saúde do pipeline"]
)

# ============================================================
# ABA 1 — SIMULAR MENSAGEM
# ============================================================
with aba_simular:
    st.subheader("Disparar simulação do pipeline")
    st.write(
        "Gera mensagens simuladas"
        "e atualiza as camadas Silver e Gold — um ciclo completo manual."
    )

    col1, col2 = st.columns(2)
    with col1:
        qtd = st.number_input("Quantidade de mensagens a simular", min_value=1, max_value=20, value=5)
    with col2:
        st.write("")
        st.write("")
        executar = st.button(" Executar ciclo completo", type="primary")

    if executar:
        with st.spinner("Simulando mensagens..."):
            simular_lote(quantidade=qtd)
        st.success(f"{qtd} mensagens simuladas e inseridas no Bronze.")

        with st.spinner("Rodando Fluxo 1 (decisão)... isso pode levar um tempo por causa do rate limit do Gemini"):
            processar_lote_pendente(limite=qtd)
        st.success("Fluxo 1 concluído.")

        with st.spinner("Atualizando Silver..."):
            processar_silver()
        st.success("Silver atualizada.")

        with st.spinner("Atualizando Gold..."):
            processar_gold()
        st.success("Gold atualizada.")

        st.balloons()

    st.divider()
    st.subheader("Últimas mensagens processadas")
    engine = get_engine()
    with engine.connect() as conn:
        df_recentes = pd.read_sql(
            text("""
                SELECT criado_em, server_id, texto, score_toxicidade, categoria, acao
                FROM bronze_eventos
                ORDER BY criado_em DESC
                LIMIT 15
            """),
            conn
        )
    st.dataframe(df_recentes, use_container_width=True, hide_index=True)

# ============================================================
# ABA 2 — DASHBOARD
# ============================================================
with aba_dashboard:
    st.subheader("Panorama geral por servidor")

    engine = get_engine()
    with engine.connect() as conn:
        df_gold = pd.read_sql(text("SELECT * FROM gold_dashboard_metrics ORDER BY toxicidade_media DESC"), conn)

    if df_gold.empty:
        st.info("Nenhuma métrica disponível ainda. Rode uma simulação na aba anterior.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Servidores monitorados", len(df_gold))
        col2.metric("Total de mensagens", int(df_gold["total_mensagens"].sum()))
        col3.metric("Total de ações tomadas", int(df_gold["total_acoes"].sum()))

        st.divider()

        col_a, col_b = st.columns(2)
        with col_a:
            st.write("**Toxicidade média por servidor**")
            st.bar_chart(df_gold.set_index("server_id")["toxicidade_media"])
        with col_b:
            st.write("**Volume de mensagens por servidor**")
            st.bar_chart(df_gold.set_index("server_id")["total_mensagens"])

        st.divider()
        st.write("**Tabela completa**")
        st.dataframe(df_gold, use_container_width=True, hide_index=True)

# ============================================================
# ABA 3 — PERFIS DE SERVIDOR
# ============================================================
with aba_perfis:
    st.subheader("Fonte de contexto consultada pela IA")
    st.write(
        "Este é o conhecimento que o classificador consulta antes de decidir — "
        "não é histórico de mensagens, é um resumo estatístico do comportamento de cada grupo."
    )

    engine = get_engine()
    with engine.connect() as conn:
        df_perfis = pd.read_sql(text("SELECT * FROM ai_server_context"), conn)

    if df_perfis.empty:
        st.info("Nenhum perfil de servidor criado ainda.")
    else:
        for _, row in df_perfis.iterrows():
            status = "🟡 Em modo treino" if row["em_modo_treino"] else "🟢 Perfil ativo"
            with st.expander(f"{row['server_id']} — {status}"):
                c1, c2 = st.columns(2)
                c1.metric("Mensagens observadas", row["mensagens_observadas"])
                c2.metric("Nível de toxicidade", row["toxicity_level"])

                st.write("**Vocabulário mapeado:**")
                st.json(row["vocab_map"])

                st.write("**Padrões de conflito conhecidos:**")
                st.json(row["conflict_patterns"])

# ============================================================
# ABA 4 — SAÚDE DO PIPELINE (Monitoramento)
# ============================================================
with aba_logs:
    st.subheader("Métricas de execução dos jobs")

    engine = get_engine()
    with engine.connect() as conn:
        df_exec = pd.read_sql(
            text("SELECT * FROM pipeline_execucoes ORDER BY executado_em DESC LIMIT 30"),
            conn
        )

    if df_exec.empty:
        st.info("Nenhuma execução de job registrada ainda.")
    else:
        col1, col2 = st.columns(2)
        col1.metric("Execuções com sucesso", int((df_exec["status"] == "sucesso").sum()))
        col2.metric("Execuções com erro", int((df_exec["status"] == "erro").sum()))
        st.dataframe(df_exec, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Log granular (últimas 100 linhas)")
    log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs", "pipeline.log")
    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            linhas = f.readlines()[-100:]
        st.code("".join(linhas), language="log")
    else:
        st.info("Arquivo de log ainda não existe.")
