import logging
import os

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(),
    ],
)


def get_logger(nome: str) -> logging.Logger:
    """Retorna um logger nomeado, configurado para gravar em logs/pipeline.log e no terminal."""
    return logging.getLogger(nome)