import os
import json
import time
from dotenv import load_dotenv
from google import genai
from sqlalchemy import text
from src.db import get_engine
from src.shared.logger import get_logger

INTERVALO_ENTRE_CHAMADAS = 13  # segundos - mantem margem segura sob 5 req/min

load_dotenv()
logger = get_logger(__name__)

_client = None


def get_client():
    global _client
    if _client is None:
        _client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    return _client


def buscar_contexto_servidor(server_id: str) -> dict:
    """Consulta o ai_server_context - a fonte de conhecimento sobre aquele grupo."""
    engine = get_engine()
    with engine.connect() as conn:
        resultado = conn.execute(
            text("SELECT * FROM ai_server_context WHERE server_id = :server_id"),
            {"server_id": server_id}
        ).mappings().first()

    if resultado is None:
        # Servidor sem perfil ainda - modo treino por padrao
        return {
            "em_modo_treino": True,
            "mensagens_observadas": 0,
            "toxicity_level": 0.0,
            "vocab_map": {},
            "conflict_patterns": [],
        }
    return dict(resultado)


def montar_prompt(texto: str, contexto: dict) -> str:
    """Monta o prompt enviado ao Gemini, incluindo o contexto do servidor."""
    return f"""Você é um classificador de moderação de chat. Analise a mensagem abaixo
considerando o CONTEXTO deste grupo especifico - a mesma frase pode ser
ofensiva em um grupo e corriqueira em outro.

CONTEXTO DO SERVIDOR:
- Nivel geral de toxicidade do grupo: {contexto.get('toxicity_level', 0.0)} (0=tranquilo, 1=muito hostil)
- Vocabulario comum deste grupo: {json.dumps(contexto.get('vocab_map', {}), ensure_ascii=False)}
- Padroes de conflito conhecidos: {json.dumps(contexto.get('conflict_patterns', []), ensure_ascii=False)}

MENSAGEM A CLASSIFICAR:
"{texto}"

Responda APENAS em JSON, sem nenhum texto antes ou depois, no formato:
{{"score": <numero de 0.0 a 1.0>, "categoria": "<ofensa_pessoal|discurso_odio|ameaca|brincadeira_comum|nenhuma>", "acao": "<ban|alerta|ignora>"}}"""


def classificar_mensagem(texto: str, server_id: str) -> dict:
    """
    Classifica a mensagem usando o Gemini, com o contexto do servidor.
    Retorna {"score": float, "categoria": str, "acao": str}.
    """
    contexto = buscar_contexto_servidor(server_id)
    prompt = montar_prompt(texto, contexto)

    try:
        time.sleep(INTERVALO_ENTRE_CHAMADAS)
        client = get_client()
        resposta = client.models.generate_content(
            model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            contents=prompt,
        )
        texto_resposta = resposta.text.strip()

        # Remove possiveis marcadores de bloco de codigo, caso o modelo adicione
        if texto_resposta.startswith("```"):
            texto_resposta = texto_resposta.strip("`").replace("json", "", 1).strip()

        resultado = json.loads(texto_resposta)
        logger.info(f"Classificador: '{texto[:40]}...' -> {resultado}")
        return resultado

    except Exception as e:
        logger.error(f"Erro ao classificar mensagem via Gemini: {e}")
        # Fallback seguro: nao decide nada automaticamente, sinaliza erro
        return {"score": 0.0, "categoria": "erro_classificacao", "acao": "ignora"}


if __name__ == "__main__":
    testes = [
        ("você é muito burro mesmo", "sv_jogos"),
        ("vai se ferrar, idiota", "sv_estudos"),
    ]
    for texto, server in testes:
        resultado = classificar_mensagem(texto, server)
        print(f"'{texto}' [{server}] -> {resultado}")
