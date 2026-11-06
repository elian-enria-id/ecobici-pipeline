"""Ejecuta el pipeline completo: raw -> bronze -> silver -> gold -> reportes."""
import sys
import time
from pathlib import Path

from src import bronze, consola, gold, reportes, resumen, silver

BASE = Path(__file__).resolve().parent

# Solo caracteres ASCII: la consola de Windows puede no mostrar bien Unicode.
TITULO = r"""
 _____   ____   ___   ____   ___   ____  ___
| ____| / ___| / _ \ | __ ) |_ _| / ___||_ _|
|  _|  | |    | | | ||  _ \  | | | |     | |
| |___ | |___ | |_| || |_) | | | | |___  | |
|_____| \____| \___/ |____/ |___| \____||___|

  raw -> bronze -> silver -> gold -> reportes
"""


def main():
    # El panel usa caracteres de recuadro (╭─╮): sin esto, redirigir la salida a un archivo falla en Windows.
    sys.stdout.reconfigure(encoding="utf-8")
    print(TITULO, flush=True)
    # Permite correr una sola etapa (ej. `python main.py gold`, o `python main.py resumen` para ver el panel).
    pedidas = sys.argv[1:] or ["bronze", "silver", "gold", "reportes", "resumen"]
    etapas = {e.__name__.split(".")[-1]: e for e in (bronze, silver, gold, reportes) if e.__name__.split(".")[-1] in pedidas}

    if etapas and sys.stdout.isatty():  # terminal: panel en vivo
        with consola.Panel(list(etapas), "python main.py " + " ".join(sys.argv[1:])) as panel:
            for nombre, etapa in etapas.items():
                with panel.etapa(nombre):
                    etapa.ejecutar(BASE)
    else:  # archivo o pipe: salida de texto simple
        for nombre, etapa in etapas.items():
            inicio = time.time()
            print(f"== {nombre}", flush=True)
            etapa.ejecutar(BASE)
            print(f"   ({time.time() - inicio:.0f} s)", flush=True)

    if "resumen" in pedidas:
        resumen.ejecutar(BASE)


if __name__ == "__main__":
    main()
