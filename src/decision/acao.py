from sqlalchemy import text
from src.db import get_engine
from src.shared.logger import get_logger

logger = get_logger(__name__)

# Regras de negocio simples sobre a acao final.
# O Gemini sugere; aqui aplicamos politica própria sobre o que ele sugeriu.
LIMIAR_BAN = 0.85
LIMIAR_ALERTA = 0.5


def decidir_acao_final(score: float, acao_sugerida: str) -> str:
    """
    Aplica politica de negocio sobre a sugestao do classificador.
    Isso evita que o pipeline dependa 100% do que o LLM "decidiu" sozinho -
    importante para auditabilidade e consistencia.
    """
    if score >= LIMIAR_BAN:
        return "ban"
    elif score >= LIMIAR_ALERTA:
        return "alerta"
    return "ignora"


def aplicar_decisao(message_id: str, score_porteiro: float, passou_porteiro: bool,
                     score_toxicidade: float = None, categoria: str = None,
                     acao: str = "nenhuma"):
    """
    Grava o resultado completo do Fluxo 1 (decisao) na linha correspondente
    do Bronze. Este e o ponto de cruzamento entre os dois fluxos.
    """
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(
            text("""
                UPDATE bronze_eventos
                SET score_porteiro = :score_porteiro,
                    passou_porteiro = :passou_porteiro,
                    score_toxicidade = :score_toxicidade,
                    categoria = :categoria,
                    acao = :acao
                WHERE message_id = :message_id
            """),
            {
                "message_id": message_id,
                "score_porteiro": score_porteiro,
                "passou_porteiro": passou_porteiro,
                "score_toxicidade": score_toxicidade,
                "categoria": categoria,
                "acao": acao,
            }
        )
        conn.commit()

    if acao != "nenhuma":
        logger.info(f"Acao aplicada: message_id={message_id} -> {acao} (categoria={categoria})")
