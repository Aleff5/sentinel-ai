import pandas as pd
from sqlalchemy import text
from src.db import get_engine
from src.shared.logger import get_logger
from src.shared.monitoring import monitorar_job

logger = get_logger(__name__)


def buscar_bronze_pendente() -> pd.DataFrame:
    """Busca linhas do Bronze que ainda nao passaram pela limpeza Silver."""
    engine = get_engine()
    query = text("""
        SELECT id, message_id, server_id, channel_id, user_id, texto,
               criado_em, score_toxicidade, categoria, acao
        FROM bronze_eventos
        WHERE processado_silver = FALSE
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def limpar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Camada de Qualidade na Silver: remove duplicata, padroniza texto,
    descarta linha com campo critico nulo. Isso e limpeza de verdade,
    nao a decisao de moderacao (essa ja aconteceu no Fluxo 1).
    """
    antes = len(df)

    df = df.drop_duplicates(subset=["message_id"])
    df = df.dropna(subset=["user_id", "texto", "server_id"])
    df = df[df["texto"].str.strip() != ""]

    df["texto_normalizado"] = df["texto"].str.strip().str.lower()
    df["hora_dia"] = pd.to_datetime(df["criado_em"]).dt.hour

    depois = len(df)
    if antes != depois:
        logger.warning(f"Silver: {antes - depois} linhas descartadas na limpeza")

    return df


def gravar_silver(df: pd.DataFrame):
    """Grava o dataframe limpo na tabela silver_eventos."""
    if df.empty:
        return

    engine = get_engine()
    registros = df[[
        "message_id", "server_id", "channel_id", "user_id",
        "texto_normalizado", "criado_em", "hora_dia",
        "score_toxicidade", "categoria", "acao"
    ]].copy()

    with engine.connect() as conn:
        for _, row in registros.iterrows():
            conn.execute(
                text("""
                    INSERT INTO silver_eventos
                    (message_id, server_id, channel_id, user_id, texto_normalizado,
                     criado_em, hora_dia, score_toxicidade, categoria, acao)
                    VALUES
                    (:message_id, :server_id, :channel_id, :user_id, :texto_normalizado,
                     :criado_em, :hora_dia, :score_toxicidade, :categoria, :acao)
                    ON CONFLICT (message_id) DO NOTHING
                """),
                row.to_dict()
            )
        conn.commit()


def marcar_processado(ids: list):
    """Marca as linhas do Bronze como ja processadas pela Silver."""
    if not ids:
        return
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE bronze_eventos SET processado_silver = TRUE WHERE id = ANY(:ids)"),
            {"ids": ids}
        )
        conn.commit()


def processar_silver():
    """Job completo: Bronze -> Silver."""
    with monitorar_job("silver") as ctx:
        df = buscar_bronze_pendente()
        if df.empty:
            logger.info("Silver: nenhuma linha pendente")
            ctx["registros"] = 0
            return

        df_limpo = limpar(df)
        gravar_silver(df_limpo)
        marcar_processado(df["id"].tolist())

        ctx["registros"] = len(df_limpo)


if __name__ == "__main__":
    processar_silver()

