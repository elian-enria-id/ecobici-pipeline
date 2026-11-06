# Pipeline Ecobici — Bronze / Silver / Gold

Ciencia de Datos — UTN FRVM. Pipeline con pandas + Parquet sobre los viajes de Ecobici 2016, 2019 y 2024.

## Requisitos

- Python 3.11 o superior (probado con 3.14)
- `pip install -r requirements.txt` (pandas y pyarrow)
- Unos 2 GB de RAM libre: los recorridos se procesan en partes de 500.000 filas, nunca completos en memoria.
- Unos 1,5 GB de disco para bronze, silver y gold.

## Datos de entrada

Descargá de <https://data.buenosaires.gob.ar/dataset/bicicletas-publicas> los recorridos y los usuarios (CSV) de 2016, 2019 y 2024, y dejalos **sin modificar** en `data/raw/`:

```
data/raw/
  recorridos-realizados-2016.zip   (o la carpeta extraída recorridos-realizados-2016/)
  recorridos-realizados-2019.zip
  recorridos-realizados-2024.zip
  usuarios-ecobici-2016.csv
  usuarios_ecobici_2019.csv
  usuarios_ecobici_2024.csv
```

El pipeline acepta cada recorrido como `.zip` (lo lee directamente con `zipfile`, sin descomprimir) o como la carpeta extraída que entregó la cátedra. Los nombres esperados están en `FUENTES`, en `src/bronze.py`.

## Ejecución

```bash
python main.py
```

Corre bronze → silver → gold → reportes de principio a fin (unos minutos). Es idempotente: cada etapa reemplaza sus archivos de salida, así que se puede ejecutar varias veces sin duplicar datos. Durante el desarrollo se puede correr una sola etapa, por ejemplo `python main.py gold`.

Mientras corre, la consola muestra una línea de avance que se reescribe en el lugar (`viajes 2019: parte 5/13...`), para saber que no se colgó.

## Tests

```bash
python test_pipeline.py
```

Prueban la limpieza de ids y nombres, el mapeo de usuarios, las reglas de cuarentena y el nombre de estación por año, con datos chicos armados a mano. No hace falta tener `data/`.

## Qué se versiona

Solo código, documentación y los 5 CSV de `data/gold/reportes/`. `data/raw/` (~2,7 GB), bronze, silver y los Parquet de gold se regeneran con `python main.py` y están en `.gitignore`.

## Salidas

```
data/bronze/{recorridos,usuarios}/anio=AAAA/data.parquet   copia fiel, todo texto + _archivo_origen, _fecha_ingesta, _anio
data/silver/viajes.parquet, viajes_cuarentena.parquet       esquema canónico; rechazados con motivo
data/silver/usuarios.parquet, usuarios_cuarentena.parquet
data/gold/fact_viajes.parquet, dim_fecha, dim_estacion, dim_usuario
data/gold/estacion_nombre_anio.parquet                      nombre de cada estación en cada año (un id puede cambiar de estación)
data/gold/reportes/*.csv                                    los 5 reportes pedidos
```

Las decisiones de diseño y la exploración inicial (Tarea 1) están en [DECISIONES.md](DECISIONES.md).
