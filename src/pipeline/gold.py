import pandas as pd
from sqlalchemy import text
from src.db import get_engine
from src.shared.logger import get_logger
from src.shared.monitoring import monitorar_job

logger = get_logger(__name__)


def buscar_silver_completa() -> pd.DataFrame:
    """Busca toda a Silver para recalcular as metricas agregadas do Gold."""
    engine = get_engine()
    query = text("SELECT * FROM silver_eventos")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def agregar_por_servidor(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agregacao por servidor - o Gold nao decide nada, so resume o que
    o Fluxo 1 ja decidiu antes.
    """
    if df.empty:
        return pd.DataFrame()

    agregado = df.groupby("server_id").agg(
        total_mensagens=("message_id", "count"),
        total_analisadas=("score_toxicidade", lambda x: x.notna().sum()),
        total_acoes=("acao", lambda x: (x != "nenhuma").sum()),
        toxicidade_media=("score_toxicidade", "mean"),
        ultima_acao_em=("criado_em", "max"),
    ).reset_index()

    agregado["toxicidade_media"] = agregado["toxicidade_media"].round(3)
    return agregado


def gravar_gold(df: pd.DataFrame):
    """Upsert das metricas no gold_dashboard_metrics - uma linha por servidor."""
    if df.empty:
        return

    engine = get_engine()
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO gold_dashboard_metrics
                    (server_id, total_mensagens, total_analisadas, total_acoes,
                     toxicidade_media, ultima_acao_em, atualizado_em)
                    VALUES
                    (:server_id, :total_mensagens, :total_analisadas, :total_acoes,
                     :toxicidade_media, :ultima_acao_em, now())
                    ON CONFLICT (server_id) DO UPDATE SET
                        total_mensagens = EXCLUDED.total_mensagens,
                        total_analisadas = EXCLUDED.total_analisadas,
                        total_acoes = EXCLUDED.total_acoes,
                        toxicidade_media = EXCLUDED.toxicidade_media,
                        ultima_acao_em = EXCLUDED.ultima_acao_em,
                        atualizado_em = now()
                """),
                row.to_dict()
            )
        conn.commit()


def processar_gold():
    """Job completo: Silver -> Gold."""
    with monitorar_job("gold") as ctx:
        df = buscar_silver_completa()
        agregado = agregar_por_servidor(df)
        gravar_gold(agregado)

        ctx["registros"] = len(agregado)
        if not agregado.empty:
            logger.info(f"Gold atualizado para {len(agregado)} servidor(es)")


if __name__ == "__main__":
    processar_gold()
