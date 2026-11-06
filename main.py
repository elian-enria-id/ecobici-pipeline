"""Ejecuta el pipeline completo: raw -> bronze -> silver -> gold -> reportes."""
import sys
import time
from pathlib import Path

from src import bronze, gold, reportes, silver

BASE = Path(__file__).resolve().parent


def main():
    # Permite correr una sola etapa (ej. `python main.py gold`) mientras se desarrolla.
    pedidas = sys.argv[1:] or ["bronze", "silver", "gold", "reportes"]
    for etapa in (bronze, silver, gold, reportes):
        nombre = etapa.__name__.split(".")[-1]
        if nombre in pedidas:
            inicio = time.time()
            print(f"== {nombre}", flush=True)
            etapa.ejecutar(BASE)
            print(f"   ({time.time() - inicio:.0f} s)", flush=True)


if __name__ == "__main__":
    main()
