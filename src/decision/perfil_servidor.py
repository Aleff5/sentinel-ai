import os
import json
import time
from dotenv import load_dotenv
from sqlalchemy import text
from src.db import get_engine
from src.decision.classificador import get_client
from src.shared.logger import get_logger
from src.shared.monitoring import monitorar_job

load_dotenv()
logger = get_logger(__name__)

TREINO_THRESHOLD = int(os.getenv("TREINO_THRESHOLD", 50))
INTERVALO_ENTRE_CHAMADAS = 13


def contar_mensagens_servidor(server_id: str) -> int:
    """Conta quantas mensagens ja foram observadas (Silver) deste servidor."""
    engine = get_engine()
    with engine.connect() as conn:
        resultado = conn.execute(
            text("SELECT COUNT(*) FROM silver_eventos WHERE server_id = :server_id"),
            {"server_id": server_id}
        ).scalar()
    return resultado or 0


def buscar_amostra_mensagens(server_id: str, limite: int = 100) -> list[str]:
    """Busca uma amostra de mensagens do servidor para a LLM extrair caracteristicas."""
    engine = get_engine()
    with engine.connect() as conn:
        resultado = conn.execute(
            text("""
                SELECT texto_normalizado FROM silver_eventos
                WHERE server_id = :server_id
                ORDER BY criado_em DESC
                LIMIT :limite
            """),
            {"server_id": server_id, "limite": limite}
        ).fetchall()
    return [r[0] for r in resultado]


def montar_prompt_extracao(mensagens: list[str]) -> str:
    """Prompt que pede a LLM para resumir o comportamento do grupo, nao classificar mensagem a mensagem."""
    amostra = "\n".join(f"- {m}" for m in mensagens)
    return f"""Você é um analista de comunidades online. Abaixo está uma amostra de
mensagens de um grupo de chat. Sua tarefa NÃO é classificar cada mensagem,
e sim resumir o COMPORTAMENTO GERAL deste grupo.

MENSAGENS:
{amostra}

Responda APENAS em JSON, sem texto antes ou depois, no formato:
{{
  "toxicity_level": <numero de 0.0 a 1.0, nivel geral de toxicidade do grupo>,
  "vocab_map": {{"<termo ou expressao frequente>": <peso de 0.0 a 1.0 de quao suspeito e>}},
  "conflict_patterns": ["<padrao textual curto que indica inicio de conflito neste grupo>"],
  "metadata": {{"tom_geral": "<descricao curta>"}}
}}"""


def extrair_perfil_via_llm(mensagens: list[str]) -> dict:
    """Chama o Gemini para extrair caracteristicas do grupo a partir da amostra."""
    client = get_client()
    prompt = montar_prompt_extracao(mensagens)

    try:
        time.sleep(INTERVALO_ENTRE_CHAMADAS)
        resposta = client.models.generate_content(
            model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            contents=prompt,
        )
        texto_resposta = resposta.text.strip()
        if texto_resposta.startswith("```"):
            texto_resposta = texto_resposta.strip("`").replace("json", "", 1).strip()
        return json.loads(texto_resposta)
    except Exception as e:
        logger.error(f"Erro ao extrair perfil via Gemini: {e}")
        return None


def salvar_ou_atualizar_perfil(server_id: str, perfil: dict, em_modo_treino: bool, mensagens_observadas: int):
    """Grava o perfil no ai_server_context - upsert."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO ai_server_context
                (server_id, em_modo_treino, mensagens_observadas, toxicity_level,
                 vocab_map, conflict_patterns, metadata, atualizado_em)
                VALUES
                (:server_id, :em_modo_treino, :mensagens_observadas, :toxicity_level,
                 :vocab_map, :conflict_patterns, :metadata, now())
                ON CONFLICT (server_id) DO UPDATE SET
                    em_modo_treino = EXCLUDED.em_modo_treino,
                    mensagens_observadas = EXCLUDED.mensagens_observadas,
                    toxicity_level = EXCLUDED.toxicity_level,
                    vocab_map = EXCLUDED.vocab_map,
                    conflict_patterns = EXCLUDED.conflict_patterns,
                    metadata = EXCLUDED.metadata,
                    atualizado_em = now()
            """),
            {
                "server_id": server_id,
                "em_modo_treino": em_modo_treino,
                "mensagens_observadas": mensagens_observadas,
                "toxicity_level": perfil.get("toxicity_level", 0.0) if perfil else 0.0,
                "vocab_map": json.dumps(perfil.get("vocab_map", {}) if perfil else {}),
                "conflict_patterns": json.dumps(perfil.get("conflict_patterns", []) if perfil else []),
                "metadata": json.dumps(perfil.get("metadata", {}) if perfil else {}),
            }
        )
        conn.commit()


def atualizar_perfil_servidor(server_id: str):
    """
    Fluxo completo: conta mensagens, decide se ainda esta em modo treino,
    e se tiver dados suficientes, faz o scan via LLM e atualiza o perfil.
    """
    total = contar_mensagens_servidor(server_id)
    em_modo_treino = total < TREINO_THRESHOLD

    if em_modo_treino:
        logger.info(f"Servidor '{server_id}' em modo treino: {total}/{TREINO_THRESHOLD} mensagens observadas")
        salvar_ou_atualizar_perfil(server_id, None, em_modo_treino=True, mensagens_observadas=total)
        return

    mensagens = buscar_amostra_mensagens(server_id)
    perfil = extrair_perfil_via_llm(mensagens)

    if perfil is None:
        logger.warning(f"Nao foi possivel extrair perfil para '{server_id}', mantendo estado anterior")
        return

    salvar_ou_atualizar_perfil(server_id, perfil, em_modo_treino=False, mensagens_observadas=total)
    logger.info(f"Perfil de '{server_id}' atualizado: toxicity_level={perfil.get('toxicity_level')}")


def atualizar_todos_os_servidores():
    """Roda a atualizacao de perfil para todos os servidores distintos na Silver."""
    engine = get_engine()
    with engine.connect() as conn:
        servidores = conn.execute(text("SELECT DISTINCT server_id FROM silver_eventos")).fetchall()

    with monitorar_job("perfil_servidor") as ctx:
        for (server_id,) in servidores:
            atualizar_perfil_servidor(server_id)
        ctx["registros"] = len(servidores)


if __name__ == "__main__":
    atualizar_todos_os_servidores()
