from sqlalchemy import text
from src.db import get_engine
from src.decision.porteiro import passa_pelo_porteiro
from src.decision.classificador import classificar_mensagem, buscar_contexto_servidor
from src.decision.acao import decidir_acao_final, aplicar_decisao
from src.shared.logger import get_logger
from src.shared.monitoring import monitorar_job

logger = get_logger(__name__)


def buscar_mensagens_nao_decididas(limite: int = 50):
    """Busca mensagens do Bronze que ainda nao passaram pelo Fluxo 1."""
    engine = get_engine()
    with engine.connect() as conn:
        resultado = conn.execute(
            text("""
                SELECT message_id, server_id, texto
                FROM bronze_eventos
                WHERE score_porteiro IS NULL
                ORDER BY criado_em ASC
                LIMIT :limite
            """),
            {"limite": limite}
        ).mappings().all()
    return [dict(r) for r in resultado]


def processar_mensagem(mensagem: dict):
    """Executa o Fluxo 1 completo para uma mensagem: porteiro -> classificador -> acao."""
    server_id = mensagem["server_id"]
    texto = mensagem["texto"]
    message_id = mensagem["message_id"]

    contexto = buscar_contexto_servidor(server_id)
    vocab_servidor = contexto.get("vocab_map", {})

    passou, score_porteiro = passa_pelo_porteiro(texto, vocab_servidor)

    if not passou:
        aplicar_decisao(message_id, score_porteiro, passou_porteiro=False, acao="nenhuma")
        return

    resultado = classificar_mensagem(texto, server_id)
    acao_final = decidir_acao_final(resultado["score"], resultado["acao"])

    aplicar_decisao(
        message_id, score_porteiro, passou_porteiro=True,
        score_toxicidade=resultado["score"], categoria=resultado["categoria"],
        acao=acao_final,
    )


def processar_lote_pendente(limite: int = 5):
    """Processa todas as mensagens pendentes do Fluxo 1, em um lote."""
    with monitorar_job("fluxo_decisao") as ctx:
        mensagens = buscar_mensagens_nao_decididas(limite)
        for msg in mensagens:
            processar_mensagem(msg)
        ctx["registros"] = len(mensagens)


if __name__ == "__main__":
    processar_lote_pendente(limite=5)
