"""Capa Bronze: copia fiel de data/raw en Parquet, todo como texto + metadatos de ingesta."""
import zipfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa

from src.parquet_util import EscritorParquet, estado

ANIOS = (2016, 2019, 2024)

# Nombre de cada fuente en data/raw, sin extensión. Puede estar como .zip (tal cual se
# descarga del portal), como .csv o como carpeta con el CSV ya extraído.
FUENTES = {
    "recorridos": {
        2016: "recorridos-realizados-2016",
        2019: "recorridos-realizados-2019",
        2024: "recorridos-realizados-2024",
    },
    "usuarios": {
        2016: "usuarios-ecobici-2016",
        2019: "usuarios_ecobici_2019",
        2024: "usuarios_ecobici_2024",
    },
}

CHUNK = 500_000


def ubicar(raw: Path, nombre: str) -> Path:
    for candidato in (raw / f"{nombre}.zip", raw / f"{nombre}.csv", raw / nombre):
        if candidato.exists():
            return candidato
    raise FileNotFoundError(f"No se encontró {nombre}(.zip|.csv|/) en {raw}")


@contextmanager
def abrir_csv(ruta: Path):
    """Devuelve (archivo o ruta legible por pd.read_csv, nombre del CSV)."""
    if ruta.suffix == ".zip":
        with zipfile.ZipFile(ruta) as z:
            # Si el ZIP trae más de un archivo, el de datos es el CSV más grande.
            miembro = max((i for i in z.infolist() if i.filename.lower().endswith(".csv")),
                          key=lambda i: i.file_size)
            with z.open(miembro) as f:
                yield f, miembro.filename
    elif ruta.is_dir():
        csv = max(ruta.glob("*.csv"), key=lambda p: p.stat().st_size)
        yield csv, csv.name
    else:
        yield ruta, ruta.name


def ingestar_archivo(ruta: Path, anio: int, destino: Path, fecha_ingesta: str) -> int:
    with abrir_csv(ruta) as (fuente, nombre_csv), \
            EscritorParquet(destino / f"anio={anio}" / "data.parquet") as escritor:
        estado(f"   leyendo {nombre_csv}...")
        # utf-8-sig: el BOM de los archivos 2024 no es parte del nombre de la columna.
        for df in pd.read_csv(fuente, dtype=str, encoding="utf-8-sig", chunksize=CHUNK):
            df["_archivo_origen"] = ruta.name if ruta.suffix == ".zip" else nombre_csv
            df["_fecha_ingesta"] = fecha_ingesta
            df["_anio"] = str(anio)
            # Esquema explícito: una columna toda nula en un chunk sigue siendo texto.
            escritor.escribir(df, pa.schema([(c, pa.string()) for c in df.columns]))
            estado(f"   leyendo {nombre_csv}: {escritor.filas:,} filas...")
    return escritor.filas


def ejecutar(base: Path) -> dict:
    raw, destino = base / "data" / "raw", base / "data" / "bronze"
    fecha_ingesta = datetime.now().isoformat(timespec="seconds")
    filas = {}
    for tabla, fuentes in FUENTES.items():
        for anio, nombre in fuentes.items():
            filas[(tabla, anio)] = ingestar_archivo(ubicar(raw, nombre), anio, destino / tabla, fecha_ingesta)
            estado(f"   bronze/{tabla}/anio={anio}: {filas[(tabla, anio)]:,} filas", fin=True)
    return filas
