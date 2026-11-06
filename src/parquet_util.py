"""Helpers de lectura/escritura Parquet por partes (los recorridos no entran en memoria)."""
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class EscritorParquet:
    """Escribe un DataFrame por partes en un único archivo Parquet.

    Escribe en un .tmp y lo renombra al cerrar: si el proceso se corta, el archivo
    anterior queda intacto, y cada ejecución reemplaza el archivo (idempotencia).
    """

    def __init__(self, ruta: Path):
        self.ruta = Path(ruta)
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        self._tmp = self.ruta.with_suffix(".tmp")
        self._writer = None
        self.filas = 0

    def escribir(self, df: pd.DataFrame, schema: pa.Schema | None = None):
        tabla = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self._tmp, tabla.schema)
        self._writer.write_table(tabla)
        self.filas += len(df)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if self._writer is not None:
            self._writer.close()
            if exc_type is None:
                self._tmp.replace(self.ruta)
            else:
                self._tmp.unlink()
        elif exc_type is None:
            # Sin filas no hay esquema para escribir; se borra el archivo de la corrida
            # anterior para no dejar datos viejos como si fueran de esta ejecución.
            self.ruta.unlink(missing_ok=True)


panel = None  # src.consola.Panel activo, si la salida es una terminal (lo maneja el propio Panel)


def estado(texto: str, fin: bool = False):
    """Línea de estado que se reescribe en el lugar; fin=True la deja fija y baja de línea."""
    if panel is not None:
        return panel.actualizar(texto, fin)
    print(f"\r{texto:<79}", end="\n" if fin else "", flush=True)


def leer_por_partes(ruta: Path, columnas: list[str] | None = None, etiqueta: str = "procesando"):
    """Itera un Parquet por row group, recuperando los dtypes de pandas guardados."""
    archivo = pq.ParquetFile(ruta)
    n = archivo.num_row_groups
    for i in range(n):
        estado(f"   {etiqueta}: parte {i + 1}/{n}...")
        yield archivo.read_row_group(i, columns=columnas).to_pandas()


def contar_filas(ruta: Path) -> int:
    return pq.ParquetFile(ruta).metadata.num_rows
