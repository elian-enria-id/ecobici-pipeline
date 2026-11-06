"""Capa Gold: modelo estrella (fact_viajes + dim_fecha, dim_estacion, dim_usuario)."""
from pathlib import Path

import numpy as np
import pandas as pd

from src.parquet_util import EscritorParquet, contar_filas, estado, leer_por_partes

DIAS_SEMANA = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]


def fecha_key(s: pd.Series) -> pd.Series:
    return (s.dt.year * 10_000 + s.dt.month * 100 + s.dt.day).astype("int32")


def construir_fact(v: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "id_viaje": v["id_viaje"],
        "fecha_key": fecha_key(v["fecha_hora_inicio"]),
        "hora_inicio": v["fecha_hora_inicio"].dt.hour.astype("int8"),
        "id_estacion_origen": v["id_estacion_origen"],
        "id_estacion_destino": v["id_estacion_destino"],
        "id_usuario": v["id_usuario"],
        "duracion_min": (v["duracion_seg"].astype("float64") / 60).round(1),
        "es_circular": (v["id_estacion_origen"] == v["id_estacion_destino"]).astype(bool),
    })


def apariciones_estaciones(v: pd.DataFrame) -> pd.DataFrame:
    """(id_estacion, nombre_estacion, anio) -> cantidad de apariciones, origen y destino juntos."""
    cols = ["id_estacion", "nombre_estacion", "anio"]
    ambas = pd.concat([
        v[["id_estacion_origen", "nombre_estacion_origen", "anio_fuente"]].set_axis(cols, axis=1),
        v[["id_estacion_destino", "nombre_estacion_destino", "anio_fuente"]].set_axis(cols, axis=1),
    ])
    return ambas.value_counts(dropna=False).rename("n").reset_index()


def construir_dim_estacion(apariciones: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Devuelve (dim_estacion, nombre por año). Un mismo id puede ser otra estación en otro año."""
    a = apariciones.groupby(["id_estacion", "nombre_estacion", "anio"], dropna=False)["n"].sum().reset_index()
    anios = a.groupby("id_estacion")["anio"].agg(primer_anio="min", ultimo_anio="max")
    # Por (id, año), el nombre más usado ese año.
    por_anio = (a.sort_values(["anio", "n"], ascending=False, na_position="last")
                 .drop_duplicates(["id_estacion", "anio"])
                 [["id_estacion", "anio", "nombre_estacion"]])
    # En dim_estacion queda el nombre del año más reciente que tenga nombre.
    nombres = (por_anio.dropna(subset=["nombre_estacion"])
                       .drop_duplicates("id_estacion")
                       .set_index("id_estacion")["nombre_estacion"])
    dim = (anios.join(nombres).reset_index()
                [["id_estacion", "nombre_estacion", "primer_anio", "ultimo_anio"]]
                .sort_values("id_estacion", ignore_index=True))
    # Si un año no trajo nombre para ese id, se usa el de dim_estacion.
    por_anio["nombre_estacion"] = por_anio["nombre_estacion"].fillna(por_anio["id_estacion"].map(nombres))
    return dim, por_anio.sort_values(["id_estacion", "anio"], ignore_index=True)


def construir_dim_fecha(desde: pd.Timestamp, hasta: pd.Timestamp) -> pd.DataFrame:
    fechas = pd.Series(pd.date_range(desde.normalize(), hasta.normalize(), freq="D"))
    return pd.DataFrame({
        "fecha_key": fecha_key(fechas),
        "fecha": fechas,
        "anio": fechas.dt.year,
        "mes": fechas.dt.month,
        "dia": fechas.dt.day,
        "dia_semana": fechas.dt.dayofweek.map(DIAS_SEMANA.__getitem__),
        "es_fin_de_semana": fechas.dt.dayofweek >= 5,
    })


def construir_dim_usuario(usuarios: pd.DataFrame) -> pd.DataFrame:
    rango = pd.cut(usuarios["edad"].astype("float64"), [15, 25, 35, 50, np.inf],
                   labels=["16-25", "26-35", "36-50", "51+"])
    return pd.DataFrame({
        "id_usuario": usuarios["id_usuario"],
        "genero": usuarios["genero"],
        "edad": usuarios["edad"],
        "rango_etario": rango.astype("string").fillna("DESCONOCIDO"),
        "anio_alta": usuarios["fecha_alta"].dt.year.astype("Int64"),
    })


def ejecutar(base: Path):
    silver, gold = base / "data" / "silver", base / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    dim_usuario = construir_dim_usuario(pd.read_parquet(silver / "usuarios.parquet"))
    ids_usuario = pd.Index(dim_usuario["id_usuario"])

    apariciones, fecha_keys, ids_estacion = [], set(), set()
    desde, hasta = pd.Timestamp.max, pd.Timestamp.min
    sin_usuario = usuario_desconocido = 0
    with EscritorParquet(gold / "fact_viajes.parquet") as escritor:
        for v in leer_por_partes(silver / "viajes.parquet", etiqueta="fact_viajes"):
            fact = construir_fact(v)
            escritor.escribir(fact)
            apariciones.append(apariciones_estaciones(v))
            desde, hasta = min(desde, v["fecha_hora_inicio"].min()), max(hasta, v["fecha_hora_inicio"].max())
            fecha_keys.update(fact["fecha_key"].unique())
            ids_estacion.update(fact["id_estacion_origen"].unique())
            ids_estacion.update(fact["id_estacion_destino"].unique())
            sin_usuario += int(fact["id_usuario"].isna().sum())
            usuario_desconocido += int((fact["id_usuario"].notna() & ~fact["id_usuario"].isin(ids_usuario)).sum())

        # Dentro del `with`: si una verificación falla, fact_viajes no se publica y no se
        # escribe ninguna dimensión, así que gold queda entero en su versión anterior.
        estado("   armando dimensiones...")
        dim_fecha = construir_dim_fecha(desde, hasta)
        dim_estacion, estacion_nombre_anio = construir_dim_estacion(pd.concat(apariciones, ignore_index=True))

        # Verificaciones del enunciado
        assert escritor.filas == contar_filas(silver / "viajes.parquet"), "fact_viajes != silver/viajes"
        assert fecha_keys <= set(dim_fecha["fecha_key"]), "fecha_key sin fila en dim_fecha"
        assert ids_estacion <= set(dim_estacion["id_estacion"]), "estación sin fila en dim_estacion"

        dim_fecha.to_parquet(gold / "dim_fecha.parquet", index=False)
        dim_estacion.to_parquet(gold / "dim_estacion.parquet", index=False)
        estacion_nombre_anio.to_parquet(gold / "estacion_nombre_anio.parquet", index=False)
        dim_usuario.to_parquet(gold / "dim_usuario.parquet", index=False)

    estado(f"   fact_viajes: {escritor.filas:,} | dim_fecha: {len(dim_fecha):,} | "
           f"dim_estacion: {len(dim_estacion):,} | dim_usuario: {len(dim_usuario):,}", fin=True)
    estado(f"   viajes sin id_usuario: {sin_usuario:,} | con id_usuario ausente en dim_usuario: {usuario_desconocido:,}", fin=True)
