import os
from dotenv import load_dotenv
from src.shared.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

THRESHOLD_PADRAO = float(os.getenv("PORTEIRO_THRESHOLD", 0.3))

# Vocabulario base de suspeita - ponto de partida.
# Sera refinado por servidor pelo ai_server_context mais adiante.
PALAVRAS_SUSPEITAS = {
    "burro": 0.6, "idiota": 0.6, "incompetente": 0.6,
    "cala a boca": 0.5, "se ferrar": 0.7, "odeio": 0.4,
    "lixo": 0.5, "merda": 0.4, "inutil": 0.5,
}


def calcular_score_porteiro(texto: str, vocab_servidor: dict = None) -> float:
    """
    Calcula um score rapido de suspeita (0 a 1) sem chamar IA externa.
    Combina o vocabulario base com o vocab_map do server_profile,
    se disponivel - isso e o que torna o porteiro sensivel ao contexto
    de cada servidor, nao so generico.
    """
    texto_lower = texto.lower()
    score_max = 0.0

    vocab_combinado = dict(PALAVRAS_SUSPEITAS)
    if vocab_servidor:
        vocab_combinado.update(vocab_servidor)

    for termo, peso in vocab_combinado.items():
        if termo in texto_lower:
            score_max = max(score_max, peso)

    return round(score_max, 3)


def passa_pelo_porteiro(texto: str, vocab_servidor: dict = None,
                          threshold: float = None) -> tuple[bool, float]:
    """
    Decide se a mensagem merece ir ao classificador.
    Retorna (passou, score). Mensagens com score baixo sao descartadas
    aqui, sem custo de chamada a IA externa.
    """
    threshold = threshold if threshold is not None else THRESHOLD_PADRAO
    score = calcular_score_porteiro(texto, vocab_servidor)
    passou = score >= threshold

    if passou:
        logger.info(f"Porteiro: mensagem passou (score={score}) -> vai para classificador")
    else:
        logger.info(f"Porteiro: mensagem descartada (score={score} < {threshold})")

    return passou, score


if __name__ == "__main__":
    testes = [
        "oi, tudo bem?",
        "você é muito burro mesmo",
        "vai se ferrar, idiota",
        "que dia lindo hoje",
    ]
    for t in testes:
        passou, score = passa_pelo_porteiro(t)
        print(f"'{t}' -> passou={passou}, score={score}")
