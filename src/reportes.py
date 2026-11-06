"""Reportes en CSV a partir de gold (el reporte 5 usa conteos de bronze/silver, como pide el enunciado)."""
from pathlib import Path

import pandas as pd

from src.bronze import ANIOS
from src.parquet_util import contar_filas, estado, leer_por_partes


def agregar_fact(gold: Path):
    """Una sola pasada por fact_viajes, acumulando lo que necesitan los reportes 1 a 4."""
    por_dia_estacion, por_usuario = [], []
    columnas = ["fecha_key", "id_estacion_origen", "id_usuario", "duracion_min", "es_circular"]
    for f in leer_por_partes(gold / "fact_viajes.parquet", columnas, "agregando fact_viajes"):
        por_dia_estacion.append(f.groupby(["fecha_key", "id_estacion_origen", "es_circular"]).size().rename("n"))
        por_usuario.append(f.groupby("id_usuario", dropna=False)["duracion_min"].agg(n="size", minutos="sum"))
    a = pd.concat(por_dia_estacion).groupby(level=[0, 1, 2]).sum().reset_index()
    u = pd.concat(por_usuario).groupby(level=0, dropna=False).sum().reset_index()
    return a, u


def viajes_por_mes(a, dim_fecha):
    return (a.merge(dim_fecha, on="fecha_key").groupby(["anio", "mes"])["n"].sum()
             .rename("cantidad_viajes").reset_index())


def top10_estaciones(a, dim_fecha, estacion_nombre_anio):
    t = (a.merge(dim_fecha, on="fecha_key").groupby(["anio", "id_estacion_origen"])["n"].sum()
          .rename("cantidad_viajes").reset_index()
          .sort_values(["anio", "cantidad_viajes", "id_estacion_origen"], ascending=[True, False, True]))
    t["ranking"] = t.groupby("anio").cumcount() + 1
    # Nombre que tenía la estación ESE año: un id puede ser otra estación en otro año.
    nombres = estacion_nombre_anio.astype({"anio": "int32"})
    t = t[t["ranking"] <= 10].merge(nombres, left_on=["id_estacion_origen", "anio"], right_on=["id_estacion", "anio"])
    return t[["anio", "ranking", "nombre_estacion", "cantidad_viajes"]].sort_values(["anio", "ranking"])


def duracion_por_rango_y_genero(u, dim_usuario):
    # Viajes sin usuario o con usuario ausente en dim_usuario -> DESCONOCIDO
    d = u.merge(dim_usuario[["id_usuario", "rango_etario", "genero"]], on="id_usuario", how="left")
    d[["rango_etario", "genero"]] = d[["rango_etario", "genero"]].fillna("DESCONOCIDO")
    r = d.groupby(["rango_etario", "genero"])[["n", "minutos"]].sum().reset_index()
    r["duracion_promedio_min"] = (r["minutos"] / r["n"]).round(1)
    return r.rename(columns={"n": "cantidad_viajes"})[["rango_etario", "genero", "cantidad_viajes", "duracion_promedio_min"]]


def viajes_circulares(a, dim_fecha):
    c = a.merge(dim_fecha, on="fecha_key")
    r = c.groupby("anio").agg(total_viajes=("n", "sum"))
    r["viajes_circulares"] = c[c["es_circular"]].groupby("anio")["n"].sum()
    r["porcentaje_circulares"] = (100 * r["viajes_circulares"] / r["total_viajes"]).round(1)
    return r.reset_index()


def calidad_por_anio(base: Path):
    bronze, silver = base / "data" / "bronze", base / "data" / "silver"
    filas = {
        "filas_silver": pd.read_parquet(silver / "viajes.parquet", columns=["anio_fuente"])["anio_fuente"].value_counts(),
        "filas_cuarentena": pd.read_parquet(silver / "viajes_cuarentena.parquet", columns=["anio_fuente"])["anio_fuente"].value_counts(),
    }
    r = pd.DataFrame({"anio": list(ANIOS)})
    r["filas_bronze"] = [contar_filas(bronze / "recorridos" / f"anio={a}" / "data.parquet") for a in ANIOS]
    for col, vc in filas.items():
        r[col] = r["anio"].map(vc).fillna(0).astype(int)
    r["porcentaje_cuarentena"] = (100 * r["filas_cuarentena"] / r["filas_bronze"]).round(2)
    return r


def ejecutar(base: Path):
    gold = base / "data" / "gold"
    salida = gold / "reportes"
    salida.mkdir(parents=True, exist_ok=True)
    dim_fecha = pd.read_parquet(gold / "dim_fecha.parquet", columns=["fecha_key", "anio", "mes"])
    estacion_nombre_anio = pd.read_parquet(gold / "estacion_nombre_anio.parquet")
    dim_usuario = pd.read_parquet(gold / "dim_usuario.parquet")
    a, u = agregar_fact(gold)

    estado("   armando reportes...")
    reportes = {
        "viajes_por_mes": viajes_por_mes(a, dim_fecha),
        "top10_estaciones_por_anio": top10_estaciones(a, dim_fecha, estacion_nombre_anio),
        "duracion_por_rango_y_genero": duracion_por_rango_y_genero(u, dim_usuario),
        "viajes_circulares": viajes_circulares(a, dim_fecha),
        "calidad_por_anio": calidad_por_anio(base),
    }
    for nombre, df in reportes.items():
        df.to_csv(salida / f"{nombre}.csv", index=False)
        estado(f"   reportes/{nombre}.csv: {len(df)} filas", fin=True)
