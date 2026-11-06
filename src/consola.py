"""Panel de avance en vivo (estilo "rollout"): se redibuja en el lugar mientras corren las etapas.

Solo se usa si la salida es una terminal; si va a un archivo o a un pipe, main.py imprime como siempre.
No toca datos: solo muestra lo que las etapas informan con estado().
"""
import os
import re
import sys
import threading
import time
from collections import deque
from contextlib import contextmanager

from src import parquet_util
from src.resumen import AZUL, BLANCO, GRIS, ROJO, VERDE, c

AMARILLO = 33
ANCHO = 80  # ancho total del recuadro
INTERIOR = ANCHO - 4
SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
LINEAS_LOG = 4


def tiempo(seg: float) -> str:
    seg = int(seg)
    return f"{seg}s" if seg < 60 else f"{seg // 60}m {seg % 60:02d}s"


def fraccion(detalle: str) -> float:
    """'viajes 2019: parte 5/13...' -> 5/13. Sin 'parte i/n' -> 0."""
    m = re.search(r"parte (\d+)/(\d+)", detalle)
    return int(m[1]) / int(m[2]) if m else 0.0


def fila(partes=()) -> str:
    plano = sum(len(t) for t, _ in partes)
    return c("│ ", GRIS) + "".join(c(t, col) for t, col in partes) + " " * (INTERIOR - plano) + c(" │", GRIS)


class Panel:
    def __init__(self, etapas: list[str], comando: str):
        self.comando = comando
        self.etapas = {e: {"estado": "pendiente", "detalle": "", "inicio": None, "seg": 0.0} for e in etapas}
        self.log = deque(maxlen=LINEAS_LOG)
        self.error = None
        self.inicio = time.monotonic()
        self._lock = threading.Lock()
        self._fin = threading.Event()
        self._dibujadas = 0

    # --- lo que llaman main.py y estado() ---

    def actualizar(self, texto: str, fin: bool):
        with self._lock:
            actual = next((e for e in self.etapas.values() if e["estado"] == "corriendo"), None)
            if actual is not None:
                actual["detalle"] = texto.strip()
            if fin:
                self.log.append(texto.strip())

    @contextmanager
    def etapa(self, nombre: str):
        e = self.etapas[nombre]
        with self._lock:
            e["estado"], e["inicio"] = "corriendo", time.monotonic()
        try:
            yield
        except BaseException as exc:
            with self._lock:
                e["estado"], self.error = "error", f"{nombre}: {exc or type(exc).__name__}"
            raise
        finally:
            e["seg"] = time.monotonic() - e["inicio"]
        with self._lock:
            e["estado"] = "listo"

    def __enter__(self):
        if os.name == "nt":
            os.system("")  # habilita los códigos ANSI en la consola clásica de Windows
        sys.stdout.write("\033[?25l")  # oculta el cursor
        parquet_util.panel = self
        self._hilo = threading.Thread(target=self._bucle, daemon=True)
        self._hilo.start()
        return self

    def __exit__(self, *_):
        self._fin.set()
        self._hilo.join()
        parquet_util.panel = None
        self._dibujar()  # cuadro final: queda en pantalla
        sys.stdout.write("\033[?25h")
        sys.stdout.flush()

    # --- dibujo ---

    def _bucle(self):
        while not self._fin.wait(0.1):
            self._dibujar()

    def _dibujar(self):
        with self._lock:
            lineas = self._lineas()
        salida = f"\033[{self._dibujadas}F" if self._dibujadas else ""
        sys.stdout.write(salida + "".join(l + "\033[K\n" for l in lineas))
        sys.stdout.flush()
        self._dibujadas = len(lineas)

    def _lineas(self) -> list[str]:
        ahora = time.monotonic()
        giro = SPINNER[int(ahora * 12) % len(SPINNER)]
        etapas = list(self.etapas.items())
        listas = sum(e["estado"] == "listo" for _, e in etapas)
        corriendo = next(((n, e) for n, e in etapas if e["estado"] == "corriendo"), None)
        terminado = self._fin.is_set()

        if self.error:
            marca = (" ✗ error ", ROJO)
        elif terminado and listas == len(etapas):
            marca = (" ✓ completo ", VERDE)
        else:
            marca = (" ● corriendo ", AMARILLO)
        titulo = " ecobici · pipeline bronze → silver → gold "
        relleno = "─" * (ANCHO - 4 - len(titulo) - len(marca[0]))
        lineas = [c("╭─", GRIS) + c(titulo, BLANCO) + c(relleno, GRIS) + c(*marca) + c("─╮", GRIS),
                  fila([("$ ", VERDE), (self.comando, BLANCO)]),
                  fila(),
                  fila([(f"{'ETAPA':<11}{'ESTADO':<14}{'DETALLE':<44}{'TIEMPO':>7}", GRIS)])]

        for nombre, e in etapas:
            seg = ahora - e["inicio"] if e["estado"] == "corriendo" else e["seg"]
            estado, col = {"pendiente": ("· pendiente", GRIS), "corriendo": (f"{giro} corriendo", AZUL),
                           "listo": ("✓ listo", VERDE), "error": ("✗ error", ROJO)}[e["estado"]]
            lineas.append(fila([(f"{nombre:<11}", GRIS if e["estado"] == "pendiente" else BLANCO),
                                (f"{estado:<14}", col),
                                (f"{e['detalle'][:42]:<44}", BLANCO if e["estado"] == "corriendo" else GRIS),
                                (f"{tiempo(seg) if e['inicio'] else '':>7}", GRIS)]))

        lineas.append(c("├" + "─" * (ANCHO - 2) + "┤", GRIS))
        bloques, nombres = [("capas    ", GRIS)], [(" " * 9, None)]
        for nombre, e in etapas:
            simbolo, col = {"pendiente": ("░░░░░░░", GRIS), "corriendo": ("▒▒▒▒▒▒▒", AMARILLO),
                            "listo": ("███████", VERDE), "error": ("███████", ROJO)}[e["estado"]]
            bloques.append((simbolo + "  ", col))
            nombres.append((f"{nombre[:8]:<9}", col))
        bloques.append((f"{listas}/{len(etapas)} publicadas", GRIS))
        lineas += [fila(bloques), fila(nombres), fila()]

        log = list(self.log) + [""] * (LINEAS_LOG - len(self.log))
        lineas += [fila([(l[:INTERIOR], GRIS)]) for l in log]
        lineas.append(fila())

        total = ahora - self.inicio
        if self.error:
            mensaje = (f"✗ {self.error}", ROJO)
        elif terminado:
            mensaje = (f"✓ pipeline completo: {len(etapas)} etapas en {tiempo(total)}", VERDE)
        elif corriendo:
            mensaje = (f"Ejecutando {corriendo[0]}... {tiempo(total)} transcurridos", AMARILLO)
        else:
            mensaje = ("Preparando...", AMARILLO)
        lineas.append(fila([(mensaje[0][:INTERIOR], mensaje[1])]))

        avance = (listas + (fraccion(corriendo[1]["detalle"]) if corriendo else 0)) / len(etapas)
        n = round(avance * 30)
        lineas.append(fila([("█" * n, VERDE), ("░" * (30 - n), GRIS), (f"  {avance:.0%}", GRIS)]))
        lineas.append(c("╰" + "─" * (ANCHO - 2) + "╯", GRIS))
        return lineas
