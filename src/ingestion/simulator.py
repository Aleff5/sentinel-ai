import uuid
from datetime import datetime, timedelta
import random
from sqlalchemy import text
from ..db import get_engine


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
    
    if random.random() < 0.8:  # 80% de mensagens benignas
        texto = random.choice(MENSAGENS_BENIGNAS)
    else:  # 20% suspeitas
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


def inserir_mensagem_no_bronze(mensagem: dict):
    """Insere uma mensagem no Bronze, com colunas de decisão ainda vazias."""
    engine = get_engine()
    
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


def simular_lote(quantidade: int):
    """Gera e insere um lote de mensagens simuladas."""
    print(f"Gerando e inserindo {quantidade} mensagens...")
    for i in range(quantidade):
        msg = gerar_mensagem()
        inserir_mensagem_no_bronze(msg)
        if (i + 1) % 10 == 0:
            print(f"  {i + 1}/{quantidade} mensagens inseridas")
    print(f"Lote de {quantidade} mensagens completo!")


if __name__ == "__main__":
    # Se executado diretamente, simula um lote pequeno para teste
    simular_lote(20)