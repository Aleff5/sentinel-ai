import uuid
from datetime import datetime, timedelta
import random
from sqlalchemy import text
from ..db import get_engine
from src.shared.logger import get_logger

logger = get_logger(__name__)

# Dados simulados para gerar mensagens realistas
SERVERS = ["sv_jogos", "sv_estudos", "sv_casual"]
USERS = [f"user_{i}" for i in range(1, 21)]
CHANNELS = {
    "sv_jogos": ["general", "gameplay", "toxicos"],
    "sv_estudos": ["geral", "duvidas", "projetos"],
    "sv_casual": ["random", "memes", "conversa"],
}

MENSAGENS_BENIGNAS = [
    "opa, tudo bem?",
    "como vocês estão?",
    "alguém aí?",
    "que legal",
    "obrigado pela ajuda",
    "concordo com você",
    "que legal essa ideia",
]

MENSAGENS_SUSPEITAS = [
    "você é burro",
    "cala a boca",
    "vai se ferrar",
    "para de ser idiota",
    "seu incompetente",
]


def gerar_mensagem():
    """Gera uma mensagem simulada aleatória."""
    server = random.choice(SERVERS)
    channel = random.choice(CHANNELS[server])
    user = random.choice(USERS)

    if random.random() < 0.8:
        texto = random.choice(MENSAGENS_BENIGNAS)
    else:
        texto = random.choice(MENSAGENS_SUSPEITAS)

    timestamp = datetime.now() - timedelta(seconds=random.randint(0, 3600))

    return {
        "message_id": str(uuid.uuid4()),
        "server_id": server,
        "channel_id": channel,
        "user_id": user,
        "texto": texto,
        "criado_em": timestamp,
    }


def validar_mensagem(mensagem: dict) -> tuple[bool, str]:
    """
    Camada de Qualidade: valida a mensagem antes de aceitar a ingestão.
    Retorna (valido, motivo). Mensagens invalidas sao rejeitadas aqui,
    nao 'corrigidas silenciosamente' depois na Silver.
    """
    if not mensagem.get("texto") or not mensagem["texto"].strip():
        return False, "texto vazio"

    if not mensagem.get("user_id"):
        return False, "user_id ausente"

    if not mensagem.get("server_id"):
        return False, "server_id ausente"

    if len(mensagem["texto"]) > 2000:
        return False, "texto excede tamanho maximo permitido"

    return True, ""


def inserir_mensagem_no_bronze(mensagem: dict) -> bool:
    """
    Insere uma mensagem no Bronze, com colunas de decisao ainda vazias.
    Retorna True se inseriu, False se rejeitada na validacao.
    """
    valido, motivo = validar_mensagem(mensagem)
    if not valido:
        logger.warning(f"Mensagem rejeitada na ingestao (qualidade): {motivo} | dados={mensagem}")
        return False

    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO bronze_eventos 
                    (message_id, server_id, channel_id, user_id, texto, criado_em, 
                     score_porteiro, passou_porteiro, score_toxicidade, categoria, acao)
                    VALUES 
                    (:message_id, :server_id, :channel_id, :user_id, :texto, :criado_em,
                     NULL, FALSE, NULL, NULL, 'nenhuma')
                """),
                {
                    "message_id": mensagem["message_id"],
                    "server_id": mensagem["server_id"],
                    "channel_id": mensagem["channel_id"],
                    "user_id": mensagem["user_id"],
                    "texto": mensagem["texto"],
                    "criado_em": mensagem["criado_em"],
                }
            )
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Erro ao inserir mensagem {mensagem['message_id']} no Bronze: {e}")
        return False


def simular_lote(quantidade: int):
    """Gera e insere um lote de mensagens simuladas."""
    logger.info(f"Iniciando simulacao de {quantidade} mensagens")
    inseridas = 0
    rejeitadas = 0

    for i in range(quantidade):
        msg = gerar_mensagem()
        if inserir_mensagem_no_bronze(msg):
            inseridas += 1
        else:
            rejeitadas += 1

    logger.info(f"Lote concluido: {inseridas} inseridas, {rejeitadas} rejeitadas")


if __name__ == "__main__":
    simular_lote(20)