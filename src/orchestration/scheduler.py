import time
from apscheduler.schedulers.background import BackgroundScheduler
from src.ingestion.simulator import simular_lote
from src.decision.engine import processar_lote_pendente
from src.pipeline.silver import processar_silver
from src.pipeline.gold import processar_gold
from src.decision.perfil_servidor import atualizar_todos_os_servidores
from src.shared.logger import get_logger

logger = get_logger(__name__)


def job_ingestao():
    """Simula a chegada de novas mensagens, como se viessem de um chat real."""
    simular_lote(quantidade=5)


def job_decisao():
    """Roda o Fluxo 1 sobre mensagens pendentes no Bronze."""
    processar_lote_pendente(limite=5)


def job_silver():
    """Bronze -> Silver."""
    processar_silver()


def job_gold():
    """Silver -> Gold."""
    processar_gold()


def job_perfil_servidor():
    """Atualiza o perfil de contexto de cada servidor."""
    atualizar_todos_os_servidores()


def criar_scheduler() -> BackgroundScheduler:
    """Monta o scheduler com todos os jobs do pipeline, em intervalos escalonados."""
    scheduler = BackgroundScheduler()

    scheduler.add_job(job_ingestao, "interval", minutes=2, id="ingestao", next_run_time=None)
    scheduler.add_job(job_decisao, "interval", minutes=3, id="decisao")
    scheduler.add_job(job_silver, "interval", minutes=5, id="silver")
    scheduler.add_job(job_gold, "interval", minutes=7, id="gold")
    scheduler.add_job(job_perfil_servidor, "interval", minutes=15, id="perfil_servidor")

    return scheduler


if __name__ == "__main__":
    scheduler = criar_scheduler()
    scheduler.start()
    logger.info("Scheduler iniciado. Jobs agendados: ingestao(2min), decisao(3min), silver(5min), gold(7min), perfil(15min)")
    logger.info("Pressione Ctrl+C para parar.")

    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler finalizado.")
