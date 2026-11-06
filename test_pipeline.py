"""Tests de la lógica de limpieza, con datos chicos armados a mano (no necesitan data/).

Correr con `python test_pipeline.py` (o `pytest test_pipeline.py` si está instalado).
"""
import tempfile
from pathlib import Path

import pandas as pd

from src.consola import fraccion, tiempo
from src.gold import construir_dim_estacion
from src.parquet_util import EscritorParquet
from src.resumen import sparkline
from src.silver import aplicar_calidad, limpiar_id, normalizar_nombre_estacion, transformar_usuarios, transformar_viajes


def test_limpiar_id():
    assert limpiar_id(pd.Series(["5BAEcobici", "992557.0", " 12 "])).tolist() == ["5", "992557", "12"]


def test_normalizar_nombre_estacion():
    s = pd.Series(["005 - Plaza  Italia ", "Pacífico", "308 - SAN MARTIN II"])
    assert normalizar_nombre_estacion(s).tolist() == ["PLAZA ITALIA", "PACIFICO", "SAN MARTIN II"]


def test_transformar_usuarios_2016():
    df = pd.DataFrame({"id_usuario": ["1", "2", "3"], "genero_usuario": ["F", " male", None],
                       "edad_usuario": ["30", "935", "4"], "fecha_alta": ["01-02-16"] * 3,
                       "hora_alta": ["3:45:19 PM"] * 3})
    u = transformar_usuarios(df, 2016)
    assert u["genero"].tolist() == ["F", "M", "DESCONOCIDO"]
    assert u["edad"].tolist()[0] == 30 and u["edad"].isna().tolist() == [False, True, True]  # fuera de 16-90 -> nulo
    assert u["fecha_alta"][0] == pd.Timestamp("2016-02-01 15:45:19")  # día-mes-año, 12 h


def test_aplicar_calidad():
    filas = [  # (id, inicio, fin, estación origen, motivo esperado)
        ("1", "2019-01-01 10:00:00", "2019-01-01 10:20:00", "5BAEcobici", None),
        ("2", "2019-01-01 10:00:00", None, "5BAEcobici", "FECHA_INVALIDA"),
        ("3", "2019-01-01 10:00:00", "2019-01-01 10:20:00", None, "ESTACION_NULA"),
        ("4", "2019-01-01 10:00:00", "2019-01-01 09:00:00", "5BAEcobici", "FIN_ANTERIOR_A_INICIO"),
        ("5", "2019-01-01 10:00:00", "2019-01-03 10:00:00", "5BAEcobici", "DURACION_MAYOR_24H"),
        ("1", "2019-01-01 10:00:00", "2019-01-01 10:20:00", "5BAEcobici", "DUPLICADO"),
    ]
    df = pd.DataFrame({
        "Id_recorrido": [f[0] for f in filas], "fecha_origen_recorrido": [f[1] for f in filas],
        "fecha_destino_recorrido": [f[2] for f in filas], "duracion_recorrido": [None] * len(filas),
        "id_estacion_origen": [f[3] for f in filas], "nombre_estacion_origen": ["005 - Plaza Italia"] * len(filas),
        "id_estacion_destino": ["7BAEcobici"] * len(filas), "nombre_estacion_destino": ["Once"] * len(filas),
        "id_usuario": ["9BAEcobici"] * len(filas),
    })
    v = transformar_viajes(df, 2019, 0)
    validos, cuarentena = aplicar_calidad(v, v["id_viaje"].duplicated())
    assert validos["id_viaje"].tolist() == ["1"] and validos["duracion_seg"].tolist() == [1200]
    assert cuarentena["motivo"].tolist() == [f[4] for f in filas if f[4]]


def test_dim_estacion_nombre_por_anio():
    # El id 41 es URQUIZA en 2016 y otra estación en 2024: cada año tiene que conservar su nombre.
    apariciones = pd.DataFrame({"id_estacion": [41, 41, 41], "anio": [2016, 2024, 2024],
                                "nombre_estacion": ["URQUIZA", "PARQUE PATRICIOS II", "PARQUE PATRICIOS"],
                                "n": [10, 50, 5]})
    dim, por_anio = construir_dim_estacion(apariciones)
    assert dim["nombre_estacion"].tolist() == ["PARQUE PATRICIOS II"]  # el más usado del año más reciente
    assert por_anio["nombre_estacion"].tolist() == ["URQUIZA", "PARQUE PATRICIOS II"]


def test_sparkline():
    assert sparkline([0, 1, 5, 10]) == " ▁▄█"  # 0 en blanco; un valor chico igual se ve


def test_panel_tiempo_y_avance():
    assert [tiempo(s) for s in (5, 59.9, 125)] == ["5s", "59s", "2m 05s"]
    assert fraccion("   viajes 2019: parte 5/10...") == 0.5 and fraccion("armando reportes...") == 0


def test_escritor_sin_filas_borra_archivo_viejo():
    with tempfile.TemporaryDirectory() as d:
        ruta = Path(d) / "x.parquet"
        with EscritorParquet(ruta) as e:
            e.escribir(pd.DataFrame({"a": [1]}))
        assert ruta.exists()
        with EscritorParquet(ruta):
            pass
        assert not ruta.exists()


if __name__ == "__main__":
    tests = [f for nombre, f in list(globals().items()) if nombre.startswith("test_")]
    for t in tests:
        t()
        print(f"ok  {t.__name__}")
    print(f"{len(tests)} tests OK")
