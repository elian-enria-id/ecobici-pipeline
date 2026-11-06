"""Panel de resumen en consola. Solo lee data/gold/reportes: no escribe ni modifica datos."""
import sys
from pathlib import Path

import pandas as pd

from src.silver import UMBRAL_CUARENTENA_VIAJES

COLOR = sys.stdout.isatty()  # sin colores si la salida va a un archivo o a un pipe
GRIS, VERDE, ROJO, AZUL, BLANCO = 90, 32, 31, 94, 97  # códigos ANSI
BARRAS = " ▁▂▃▄▅▆▇█"

# (título, ancho, alineación)
COLUMNAS = [("Año", 6, "<"), ("Viajes", 12, ">"), ("Cuarentena", 12, ">"), ("Circulares", 12, ">"),
            ("   Ene → Dic", 17, "<"), ("Estación top", 19, "<"), ("Estado", 7, "<")]
ANCHO = sum(a for _, a, _ in COLUMNAS)


def c(texto: str, color: int | None) -> str:
    return f"\033[{color}m{texto}\033[0m" if COLOR and color else texto


def miles(n) -> str:
    return f"{int(n):,}".replace(",", ".")


def porcentaje(x) -> str:
    return f"{x:.2f} %".replace(".", ",")


def sparkline(valores) -> str:
    """Una barra por valor, escalada al máximo; 0 queda en blanco."""
    m = max(valores) or 1
    return "".join(BARRAS[0 if v == 0 else max(1, round(v / m * (len(BARRAS) - 1)))] for v in valores)


def linea(partes) -> str:
    """Línea interior del recuadro. partes = [(texto, color), ...]; el relleno se calcula sin los códigos ANSI."""
    plano = sum(len(t) for t, _ in partes)
    return c("│ ", GRIS) + "".join(c(t, col) for t, col in partes) + " " * (ANCHO - plano) + c(" │", GRIS)


def borde(izq: str, der: str, titulo=("", None), estado=("", None)) -> str:
    relleno = "─" * (ANCHO + 2 - len(titulo[0]) - len(estado[0]) - 2)
    return c(izq + "─", GRIS) + c(*titulo) + c(relleno, GRIS) + c(*estado) + c("─" + der, GRIS)


def ejecutar(base: Path):
    r = base / "data" / "gold" / "reportes"
    calidad = pd.read_csv(r / "calidad_por_anio.csv").set_index("anio")
    circulares = pd.read_csv(r / "viajes_circulares.csv").set_index("anio")["porcentaje_circulares"]
    top = pd.read_csv(r / "top10_estaciones_por_anio.csv").query("ranking == 1").set_index("anio")["nombre_estacion"]
    por_mes = (pd.read_csv(r / "viajes_por_mes.csv").pivot(index="anio", columns="mes", values="cantidad_viajes")
                 .reindex(columns=range(1, 13)).fillna(0))
    duracion = pd.read_csv(r / "duracion_por_rango_y_genero.csv")

    ok = {a: calidad.at[a, "porcentaje_cuarentena"] <= 100 * UMBRAL_CUARENTENA_VIAJES[a] for a in calidad.index}
    todo_ok = all(ok.values())

    print()
    print(borde("╭", "╮", (" Ecobici · resumen ", BLANCO),
                (" ● pipeline OK ", VERDE) if todo_ok else (" ● revisar cuarentena ", ROJO)))
    print(linea([(f"{t:{al}{an}}", AZUL if t == "Año" else GRIS) for t, an, al in COLUMNAS]))
    print(linea([("─" * ANCHO, GRIS)]))
    for anio in calidad.index:
        celdas = [
            (str(anio), BLANCO),
            (miles(calidad.at[anio, "filas_silver"]), None),
            (porcentaje(calidad.at[anio, "porcentaje_cuarentena"]), None if ok[anio] else ROJO),
            (porcentaje(circulares[anio]), None),
            ("   " + sparkline(por_mes.loc[anio].tolist()), AZUL),
            (top[anio][:COLUMNAS[5][1] - 1], None),
            ("● ok" if ok[anio] else "● alto", VERDE if ok[anio] else ROJO),
        ]
        print(linea([(f"{t:{al}{an}}", col) for (t, col), (_, an, al) in zip(celdas, COLUMNAS)]))
    print(linea([("─" * ANCHO, GRIS)]))
    umbrales = ", ".join(f"{a}: {u:.0%}" for a, u in UMBRAL_CUARENTENA_VIAJES.items())
    print(linea([(f"cuarentena = filas rechazadas en silver · umbral {umbrales}", GRIS)]))
    print(linea([]))

    viajes, rechazadas = calidad["filas_silver"].sum(), calidad["filas_cuarentena"].sum()
    media = (duracion["cantidad_viajes"] * duracion["duracion_promedio_min"]).sum() / duracion["cantidad_viajes"].sum()
    chips = [("Viajes en gold", miles(viajes), BLANCO),
             ("En cuarentena", miles(rechazadas), BLANCO),
             ("Duración media", f"{media:.1f} min".replace(".", ","), BLANCO),
             ("Años", " · ".join(map(str, calidad.index)), AZUL)]
    print(linea([(f"{t:<18}", GRIS) for t, _, _ in chips]))
    print(linea([(f"{v:<18}", col) for _, v, col in chips]))
    print(borde("╰", "╯"))
