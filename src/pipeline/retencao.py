import os
from dotenv import load_dotenv
from sqlalchemy import text
from src.db import get_engine
from src.shared.logger import get_logger
from src.shared.monitoring import monitorar_job

load_dotenv()
logger = get_logger(__name__)

# Política de retenção (Governança). Valores menores que os da arquitetura
# visionária (90/180 dias) porque o MVP roda em janela curta de demonstração -
# a política é a mesma em espírito, escalada para o contexto do protótipo.
RETENCAO_BRONZE_DIAS = int(os.getenv("RETENCAO_BRONZE_DIAS", 30))
RETENCAO_SILVER_DIAS = int(os.getenv("RETENCAO_SILVER_DIAS", 60))


def aplicar_retencao_bronze() -> int:
    """
    Remove eventos do Bronze mais antigos que RETENCAO_BRONZE_DIAS.
    Eventos com acao != 'nenhuma' (ban/alerta) sao preservados mesmo
    apos expirar - rastreabilidade de decisao automatizada (Governanca)
    exige que ações tomadas continuem auditáveis por mais tempo que o
    tráfego benigno.
    """
    engine = get_engine()
    with engine.connect() as conn:
        resultado = conn.execute(
            text("""
                DELETE FROM bronze_eventos
                WHERE criado_em < now() - (:dias || ' days')::interval
                  AND acao = 'nenhuma'
                RETURNING id
            """),
            {"dias": RETENCAO_BRONZE_DIAS}
        )
        removidos = resultado.rowcount
        conn.commit()
    return removidos


def aplicar_retencao_silver() -> int:
    """Mesma lógica para a Silver, com janela mais longa (dado já limpo, custo de manter é menor)."""
    engine = get_engine()
    with engine.connect() as conn:
        resultado = conn.execute(
            text("""
                DELETE FROM silver_eventos
                WHERE criado_em < now() - (:dias || ' days')::interval
                  AND acao = 'nenhuma'
                RETURNING id
            """),
            {"dias": RETENCAO_SILVER_DIAS}
        )
        removidos = resultado.rowcount
        conn.commit()
    return removidos


def aplicar_politica_retencao():
    """Job de Governança: aplica retenção e registra a execução para auditoria."""
    with monitorar_job("retencao_dados") as ctx:
        removidos_bronze = aplicar_retencao_bronze()
        removidos_silver = aplicar_retencao_silver()

        total = removidos_bronze + removidos_silver
        ctx["registros"] = total

        logger.info(
            f"Retenção aplicada: {removidos_bronze} eventos removidos do Bronze "
            f"(>{RETENCAO_BRONZE_DIAS}d), {removidos_silver} da Silver (>{RETENCAO_SILVER_DIAS}d)"
        )


if __name__ == "__main__":
    aplicar_politica_retencao()