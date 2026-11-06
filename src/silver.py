"""Capa Silver: esquema canónico, tipos correctos, valores normalizados y cuarentena."""
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.bronze import ANIOS
from src.parquet_util import EscritorParquet, contar_filas, estado, leer_por_partes

# --- Esquemas canónicos -----------------------------------------------------------

ESQUEMA_VIAJES = {
    "id_viaje": "string",
    "fecha_hora_inicio": "datetime64[ns]",
    "fecha_hora_fin": "datetime64[ns]",
    "duracion_seg": "Int64",
    "id_estacion_origen": "Int64",
    "nombre_estacion_origen": "string",
    "id_estacion_destino": "Int64",
    "nombre_estacion_destino": "string",
    "id_usuario": "string",
    "anio_fuente": "Int64",
}
ESQUEMA_CUARENTENA = {**ESQUEMA_VIAJES, "motivo": "string"}

ESQUEMA_USUARIOS = {
    "id_usuario": "string",
    "genero": "string",
    "edad": "Int64",
    "fecha_alta": "datetime64[ns]",
    "anio_fuente": "Int64",
}
ESQUEMA_USUARIOS_CUARENTENA = {**ESQUEMA_USUARIOS, "motivo": "string"}

# --- Mapeos por año (todo lo que cambia entre años vive acá) ----------------------

_ESTACIONES = {
    "id_estacion_origen": "id_estacion_origen",
    "nombre_estacion_origen": "nombre_estacion_origen",
    "id_estacion_destino": "id_estacion_destino",
    "nombre_estacion_destino": "nombre_estacion_destino",
}
MAPEO_VIAJES = {
    # 2016 no trae id de viaje ni de usuario; la duración viene como timedelta de texto.
    2016: {
        "fecha_origen_recorrido": "fecha_hora_inicio",
        "fecha_destino_recorrido": "fecha_hora_fin",
        **_ESTACIONES,
    },
    2019: {
        "Id_recorrido": "id_viaje",
        "fecha_origen_recorrido": "fecha_hora_inicio",
        "fecha_destino_recorrido": "fecha_hora_fin",
        "duracion_recorrido": "duracion_seg",
        **_ESTACIONES,
        "id_usuario": "id_usuario",
    },
    2024: {
        "id_recorrido": "id_viaje",
        "fecha_origen_recorrido": "fecha_hora_inicio",
        "fecha_destino_recorrido": "fecha_hora_fin",
        "duracion_recorrido": "duracion_seg",
        **_ESTACIONES,
        "id_usuario": "id_usuario",
    },
}
FORMATO_FECHA_VIAJES = {2016: "%Y-%m-%d %H:%M:%S", 2019: "%Y-%m-%d %H:%M:%S", 2024: "%Y-%m-%d %H:%M:%S"}

MAPEO_USUARIOS = {
    2016: {"id_usuario": "id_usuario", "genero_usuario": "genero", "edad_usuario": "edad",
           "fecha_alta": "fecha_alta", "hora_alta": "hora_alta"},
    2019: {"ID_usuario": "id_usuario", "genero_usuario": "genero", "edad_usuario": "edad",
           "fecha_alta": "fecha_alta", "hora_alta": "hora_alta"},
    2024: {"id_usuario": "id_usuario", "genero_usuario": "genero", "edad_usuario": "edad",
           "fecha_alta": "fecha_alta", "hora_alta": "hora_alta"},
}
# 2016: "01-01-16" + "3:45:19 AM" (día-mes-año de 2 dígitos, reloj de 12 h).
FORMATO_ALTA = {2016: "%d-%m-%y %I:%M:%S %p", 2019: "%Y-%m-%d %H:%M:%S", 2024: "%Y-%m-%d %H:%M:%S"}

MAPEO_GENERO = {
    "F": "F", "FEMALE": "F", "FEMENINO": "F",
    "M": "M", "MALE": "M", "MASCULINO": "M",
    "OTHER": "OTRO", "OTRO": "OTRO", "X": "OTRO",
}  # cualquier otro valor (incluido nulo) -> DESCONOCIDO

EDAD_MIN, EDAD_MAX = 16, 90
DURACION_MAX_SEG = 86_400

# Proporción máxima de filas en cuarentena antes de frenar el pipeline. La conciliación
# no detecta que un año entero termine en cuarentena (p. ej. si cambia el formato de fecha).
# 2016 tiene 15,7 % esperable (filas sin fecha de destino); 2019 y 2024 rondan el 0,1 %.
UMBRAL_CUARENTENA_VIAJES = {2016: 0.20, 2019: 0.05, 2024: 0.05}
UMBRAL_CUARENTENA_USUARIOS = 0.05  # hoy 1,6 % (duplicados 2016/2024)


# --- Transformaciones -------------------------------------------------------------

def exigir_columnas(ruta: Path, columnas) -> None:
    """Contrato con la fuente: si falta una columna esperada, falla con un mensaje claro."""
    faltan = set(columnas) - set(pq.ParquetFile(ruta).schema_arrow.names)
    if faltan:
        raise ValueError(f"{ruta}: faltan columnas {sorted(faltan)}. ¿La fuente cambió? Revisar MAPEO_* en silver.py")


def renombrar(df: pd.DataFrame, mapeo: dict) -> pd.DataFrame:
    return df.rename(columns=mapeo)[list(mapeo.values())]  # descarta lo que no está en el mapeo


def limpiar_id(s: pd.Series) -> pd.Series:
    """'5BAEcobici' (2019) -> '5'; '992557.0' (2024, exportado como float) -> '992557'."""
    return s.str.strip().str.replace(r"(BAEcobici|\.0)$", "", regex=True)


def normalizar_nombre_estacion(s: pd.Series) -> pd.Series:
    """'005 - Plaza  Italia ' -> 'PLAZA ITALIA'; 'Pacífico' -> 'PACIFICO'."""
    return (s.str.normalize("NFKD").str.encode("ascii", "ignore").str.decode("ascii")  # sin tildes
             .str.upper()
             .str.replace(r"^\s*\d+\s*-\s*", "", regex=True)  # prefijo numérico de 2019/2024
             .str.replace(r"\s+", " ", regex=True)
             .str.strip()
             .astype("string"))


def transformar_viajes(df: pd.DataFrame, anio: int, fila_inicial: int) -> pd.DataFrame:
    df = renombrar(df, MAPEO_VIAJES[anio])
    formato = FORMATO_FECHA_VIAJES[anio]
    inicio = pd.to_datetime(df["fecha_hora_inicio"], format=formato, errors="coerce")
    fin = pd.to_datetime(df["fecha_hora_fin"], format=formato, errors="coerce")

    if "id_viaje" in df:
        id_viaje = limpiar_id(df["id_viaje"])
    else:  # id sintético y estable: año + posición de la fila en el archivo original
        id_viaje = pd.Series(np.arange(fila_inicial, fila_inicial + len(df)), index=df.index).map(f"{anio}-{{}}".format)

    duracion = (fin - inicio).dt.total_seconds()
    if "duracion_seg" in df:  # 2019 usa coma de miles: "2,814"
        origen = pd.to_numeric(df["duracion_seg"].str.replace(",", "", regex=False), errors="coerce")
        duracion = origen.fillna(duracion)

    return pd.DataFrame({
        "id_viaje": id_viaje.astype("string"),
        "fecha_hora_inicio": inicio.astype("datetime64[ns]"),
        "fecha_hora_fin": fin.astype("datetime64[ns]"),
        "duracion_seg": duracion.round().astype("Int64"),
        "id_estacion_origen": pd.to_numeric(limpiar_id(df["id_estacion_origen"]), errors="coerce").astype("Int64"),
        "nombre_estacion_origen": normalizar_nombre_estacion(df["nombre_estacion_origen"]),
        "id_estacion_destino": pd.to_numeric(limpiar_id(df["id_estacion_destino"]), errors="coerce").astype("Int64"),
        "nombre_estacion_destino": normalizar_nombre_estacion(df["nombre_estacion_destino"]),
        "id_usuario": (limpiar_id(df["id_usuario"]) if "id_usuario" in df else pd.Series(pd.NA, index=df.index)).astype("string"),
        "anio_fuente": pd.Series(anio, index=df.index, dtype="Int64"),
    })


def aplicar_calidad(df: pd.DataFrame, duplicado: pd.Series):
    """Devuelve (validos, cuarentena). Cada fila rechazada lleva el primer motivo que falla."""
    motivo = pd.Series(pd.NA, index=df.index, dtype="string")
    reglas = [
        ("FECHA_INVALIDA", df["fecha_hora_inicio"].isna() | df["fecha_hora_fin"].isna()),
        ("ESTACION_NULA", df["id_estacion_origen"].isna() | df["id_estacion_destino"].isna()),
        ("FIN_ANTERIOR_A_INICIO", df["fecha_hora_fin"] < df["fecha_hora_inicio"]),
        ("DURACION_MAYOR_24H", (df["duracion_seg"] > DURACION_MAX_SEG).fillna(False)),
        ("DUPLICADO", duplicado),
    ]
    for nombre, condicion in reglas:
        motivo = motivo.mask(motivo.isna() & condicion, nombre)
    validos = df[motivo.isna()]
    cuarentena = df[motivo.notna()].assign(motivo=motivo[motivo.notna()])
    return validos, cuarentena


def validar_esquema(df: pd.DataFrame, esquema: dict):
    actual = {c: str(t) for c, t in df.dtypes.items()}
    if actual != esquema or list(df.columns) != list(esquema):
        raise ValueError(f"Esquema inválido.\nEsperado: {esquema}\nObtenido: {actual}")


def hash_ids(s: pd.Series) -> np.ndarray:
    # ponytail: dedup global por hash de 64 bits (colisión ~1e-5 con 10M ids); exacto si hiciera falta = set de strings
    return pd.util.hash_pandas_object(s, index=False).to_numpy()


def procesar_viajes(bronze: Path, silver: Path) -> dict:
    """Las verificaciones corren dentro del `with`: si una falla, no se publica nada y queda la versión anterior."""
    conteos = {}
    vistos = np.array([], dtype=np.uint64)  # hashes de id_viaje ya procesados (ordenados)
    hashes_validos = []
    with EscritorParquet(silver / "viajes.parquet") as ok, \
            EscritorParquet(silver / "viajes_cuarentena.parquet") as cuar:
        for anio in ANIOS:
            ruta = bronze / "recorridos" / f"anio={anio}" / "data.parquet"
            exigir_columnas(ruta, MAPEO_VIAJES[anio])
            n_ok, n_cuar, fila = ok.filas, cuar.filas, 0
            for parte in leer_por_partes(ruta, list(MAPEO_VIAJES[anio]), f"viajes {anio}"):
                df = transformar_viajes(parte, anio, fila)
                fila += len(df)
                h = hash_ids(df["id_viaje"])
                duplicado = pd.Series(np.isin(h, vistos) | pd.Series(h).duplicated().to_numpy(), index=df.index)
                vistos = np.union1d(vistos, h)
                validos, cuarentena = aplicar_calidad(df, duplicado)
                validar_esquema(validos, ESQUEMA_VIAJES)
                validar_esquema(cuarentena, ESQUEMA_CUARENTENA)
                ok.escribir(validos)
                cuar.escribir(cuarentena)
                hashes_validos.append(hash_ids(validos["id_viaje"]))
            c = conteos[anio] = {"bronze": contar_filas(ruta), "silver": ok.filas - n_ok, "cuarentena": cuar.filas - n_cuar}
            estado(f"   viajes {anio}: {c}", fin=True)
            assert c["bronze"] == c["silver"] + c["cuarentena"], f"Conciliación fallida en {anio}: {c}"
            assert c["cuarentena"] <= UMBRAL_CUARENTENA_VIAJES[anio] * c["bronze"], \
                f"Cuarentena de viajes {anio} supera el {UMBRAL_CUARENTENA_VIAJES[anio]:.0%}: {c}"
        estado("   verificando: unicidad de id_viaje...")
        h = np.concatenate(hashes_validos)
        assert len(np.unique(h)) == len(h), "id_viaje no es único en silver/viajes"
    return conteos


def transformar_usuarios(df: pd.DataFrame, anio: int) -> pd.DataFrame:
    df = renombrar(df, MAPEO_USUARIOS[anio])
    edad = pd.to_numeric(df["edad"], errors="coerce").astype("Int64")
    return pd.DataFrame({
        "id_usuario": limpiar_id(df["id_usuario"]).astype("string"),
        "genero": df["genero"].str.strip().str.upper().map(MAPEO_GENERO).fillna("DESCONOCIDO").astype("string"),
        "edad": edad.where(edad.between(EDAD_MIN, EDAD_MAX)),  # fuera de rango -> nulo
        "fecha_alta": pd.to_datetime(df["fecha_alta"] + " " + df["hora_alta"], format=FORMATO_ALTA[anio],
                                     errors="coerce").astype("datetime64[ns]"),
        "anio_fuente": pd.Series(anio, index=df.index, dtype="Int64"),
    })


def procesar_usuarios(bronze: Path, silver: Path) -> dict:
    estado("   usuarios: procesando...")
    rutas = {a: bronze / "usuarios" / f"anio={a}" / "data.parquet" for a in ANIOS}
    for a, ruta in rutas.items():
        exigir_columnas(ruta, MAPEO_USUARIOS[a])
    df = pd.concat([transformar_usuarios(pd.read_parquet(ruta), a) for a, ruta in rutas.items()], ignore_index=True)
    motivo = pd.Series(pd.NA, index=df.index, dtype="string")
    motivo = motivo.mask(df["id_usuario"].isna(), "ID_NULO")
    # Ante duplicados queda el alta más reciente; el resto va a cuarentena.
    mas_reciente_primero = df.sort_values("fecha_alta", ascending=False, na_position="last", kind="stable")
    duplicado = mas_reciente_primero["id_usuario"].duplicated() & mas_reciente_primero["id_usuario"].notna()
    motivo = motivo.mask(motivo.isna() & duplicado.reindex(df.index), "DUPLICADO")

    validos = df[motivo.isna()]
    cuarentena = df[motivo.notna()].assign(motivo=motivo[motivo.notna()])
    validar_esquema(validos, ESQUEMA_USUARIOS)
    validar_esquema(cuarentena, ESQUEMA_USUARIOS_CUARENTENA)
    conteos = {"bronze": len(df), "silver": len(validos), "cuarentena": len(cuarentena)}
    assert conteos["bronze"] == conteos["silver"] + conteos["cuarentena"], f"Conciliación fallida en usuarios: {conteos}"
    assert validos["id_usuario"].is_unique, "id_usuario no es único en silver/usuarios"
    assert conteos["cuarentena"] <= UMBRAL_CUARENTENA_USUARIOS * conteos["bronze"], \
        f"Cuarentena de usuarios supera el {UMBRAL_CUARENTENA_USUARIOS:.0%}: {conteos}"
    # Se escribe recién después de verificar.
    validos.to_parquet(silver / "usuarios.parquet", index=False)
    cuarentena.to_parquet(silver / "usuarios_cuarentena.parquet", index=False)
    estado(f"   usuarios: {conteos}", fin=True)
    return conteos


def ejecutar(base: Path):
    bronze, silver = base / "data" / "bronze", base / "data" / "silver"
    silver.mkdir(parents=True, exist_ok=True)
    procesar_viajes(bronze, silver)
    procesar_usuarios(bronze, silver)
    estado("   verificación OK: conciliación bronze = silver + cuarentena, ids únicos, esquema canónico", fin=True)
