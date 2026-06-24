import time
from contextlib import contextmanager
from sqlalchemy import text
from src.db import get_engine
from src.shared.logger import get_logger

logger = get_logger(__name__)


def registrar_execucao(job_nome: str, status: str, registros_processados: int = 0,
                        duracao_segundos: float = None, erro_mensagem: str = None):
    """Grava uma linha em pipeline_execucoes — a métrica de saúde que o dashboard vai exibir."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO pipeline_execucoes
                    (job_nome, status, registros_processados, duracao_segundos, erro_mensagem)
                    VALUES (:job_nome, :status, :registros_processados, :duracao_segundos, :erro_mensagem)
                """),
                {
                    "job_nome": job_nome,
                    "status": status,
                    "registros_processados": registros_processados,
                    "duracao_segundos": duracao_segundos,
                    "erro_mensagem": erro_mensagem,
                }
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Falha ao registrar execucao do job {job_nome}: {e}")


@contextmanager
def monitorar_job(job_nome: str):
    """
    Context manager que mede duracao, captura erro e registra tudo em
    pipeline_execucoes automaticamente. Uso:

        with monitorar_job("silver") as ctx:
            ... processamento ...
            ctx["registros"] = 340
    """
    inicio = time.time()
    contexto = {"registros": 0}
    logger.info(f"Job '{job_nome}' iniciado")
    try:
        yield contexto
        duracao = time.time() - inicio
        registrar_execucao(job_nome, "sucesso", contexto["registros"], duracao)
        logger.info(f"Job '{job_nome}' concluido: {contexto['registros']} registros em {duracao:.2f}s")
    except Exception as e:
        duracao = time.time() - inicio
        registrar_execucao(job_nome, "erro", contexto["registros"], duracao, str(e))
        logger.error(f"Job '{job_nome}' falhou apos {duracao:.2f}s: {e}")
        raise