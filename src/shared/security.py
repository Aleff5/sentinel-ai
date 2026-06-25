import hashlib
import os
from dotenv import load_dotenv

load_dotenv()

# Salt fixo do projeto - garante que o mesmo user_id sempre mascara para o
# mesmo hash (permite comparar "quantas mensagens do mesmo usuário" sem
# nunca expor o user_id real). Em produção, isso viria de um segredo
# gerenciado (Vault), nunca hardcoded - aqui vem do .env (Seguranca minima
# adequada ao MVP).
SALT = os.getenv("MASK_SALT", "sentinel-ai-salt-trocar-em-producao")


def mascarar_user_id(user_id: str) -> str:
    """
    Mascara o user_id antes de qualquer exibicao externa (dashboard).
    Usa hash determinístico com salt - o mesmo usuário sempre gera o
    mesmo identificador mascarado, mas o valor original não é reversível
    a partir do hash.
    """
    if not user_id:
        return user_id
    hash_completo = hashlib.sha256(f"{SALT}{user_id}".encode()).hexdigest()
    return f"user_{hash_completo[:8]}"