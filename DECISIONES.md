# DECISIONES

## 1. Exploración de los archivos (Tarea 1)

La cátedra entregó los recorridos ya extraídos (una carpeta por año con un CSV). El pipeline también acepta el `.zip` original (ver `abrir_csv` en `src/bronze.py`).

### Recorridos

| Aspecto | 2016 | 2019 | 2024 |
|---|---|---|---|
| Archivo CSV (dentro del ZIP / carpeta) | `recorridos-realizados-2016.csv` | `trips_2019.csv` | `badata_ecobici_recorridos_realizados_2024.csv` |
| Separador | `,` | `,` (todos los campos entre comillas) | `,` (UTF-8 con BOM) |
| Columnas | `periodo, genero_usuario, fecha_origen_recorrido, id_estacion_origen, nombre_estacion_origen, long_estacion_origen, lat_estacion_origen, domicilio_estacion_origen, duracion_recorrido, fecha_destino_recorrido, id_estacion_destino, nombre_estacion_destino, long_estacion_destino, lat_estacion_destino, domicilio_estacion_destino` (15) | `"" (índice sin nombre → Unnamed: 0), Id_recorrido, duracion_recorrido, fecha_origen_recorrido, id_estacion_origen, nombre_estacion_origen, direccion_estacion_origen, long_estacion_origen, lat_estacion_origen, fecha_destino_recorrido, id_estacion_destino, nombre_estacion_destino, direccion_estacion_destino, long_estacion_destino, lat_estacion_destino, id_usuario, modelo_bicicleta, género` (18) | `id_recorrido, duracion_recorrido, fecha_origen_recorrido, id_estacion_origen, nombre_estacion_origen, direccion_estacion_origen, long_estacion_origen, lat_estacion_origen, fecha_destino_recorrido, id_estacion_destino, nombre_estacion_destino, direccion_estacion_destino, long_estacion_destino, lat_estacion_destino, id_usuario, modelo_bicicleta, genero` (17) |
| Formato fecha y hora | `2016-01-01 02:21:41` | `2019-02-27 19:44:42` | `2024-01-23 18:36:00` |
| ¿Trae duración? Unidad | Sí, como timedelta de texto (`0 days 00:16:29.000000000`) y **no coincide con las fechas** (ver §4) | Sí, en segundos con coma de miles (`"2,814"`) | Sí, en segundos (`568`) |
| ¿Trae id de usuario? | **No** (solo género) | Sí, con sufijo (`6243BAEcobici`) | Sí, exportado como float (`992557.0`) |
| ¿Trae id de viaje? | **No** | Sí, con sufijo (`9379BAEcobici`) | Sí (`20428222`) |
| Id de estación | `11` | `5BAEcobici` | `513` |
| Nombre de estación | `Tribunales` | `005 - Plaza Italia` | `308 - SAN MARTIN II` |
| Cantidad de filas | 596.807 | 6.217.004 | 3.559.284 |

Otras observaciones: en 2019, `long_estacion_destino` trae la latitud y `lat_estacion_destino` trae "lat,long" juntas (no se usan en el esquema canónico). Las últimas 93.555 filas de 2016 (desde el 27/06/2016) no tienen `fecha_destino_recorrido`.

### Usuarios

| Aspecto | 2016 | 2019 | 2024 |
|---|---|---|---|
| Archivo | `usuarios-ecobici-2016.csv` | `usuarios_ecobici_2019.csv` | `usuarios_ecobici_2024.csv` |
| Separador | `,` | `,` (campos entre comillas) | `,` (UTF-8 con BOM) |
| Columnas | `id_usuario, genero_usuario, edad_usuario, fecha_alta, hora_alta` | `ID_usuario` (mayúsculas), resto igual | igual que 2016 |
| Formato fecha / hora de alta | `01-01-16` (día-mes-año de 2 dígitos) / `3:45:19 AM` (12 h) | `2019-12-31` / `10:46:10` | `2024-01-18` / `09:21:02` |
| Valores de género | `M`, `F` | `MALE`, `FEMALE`, `OTHER`, vacío (857) | `MALE`, `FEMALE`, `OTHER` |
| Edades fuera de 16–90 | 0 | 46 (mín. 0, máx. 935) | 94 (mín. 4, máx. 2021) |
| Cantidad de filas | 39.481 | 83.600 | 197.077 |

## 2. Mapeos

### Columnas de viajes (`MAPEO_VIAJES` en `src/silver.py`)

| Canónica | 2016 | 2019 | 2024 |
|---|---|---|---|
| id_viaje | — (se genera) | `Id_recorrido` | `id_recorrido` |
| fecha_hora_inicio | `fecha_origen_recorrido` | `fecha_origen_recorrido` | `fecha_origen_recorrido` |
| fecha_hora_fin | `fecha_destino_recorrido` | `fecha_destino_recorrido` | `fecha_destino_recorrido` |
| duracion_seg | — (se calcula) | `duracion_recorrido` | `duracion_recorrido` |
| id_estacion_origen / destino | `id_estacion_origen` / `id_estacion_destino` | ídem | ídem |
| nombre_estacion_origen / destino | `nombre_estacion_origen` / `nombre_estacion_destino` | ídem | ídem |
| id_usuario | — (nulo) | `id_usuario` | `id_usuario` |

### Columnas de usuarios (`MAPEO_USUARIOS`)

`id_usuario` (`ID_usuario` en 2019) → `id_usuario`; `genero_usuario` → `genero`; `edad_usuario` → `edad`; `fecha_alta` + `hora_alta` → `fecha_alta`.

### Género (`MAPEO_GENERO`)

| Original | Silver |
|---|---|
| `F`, `FEMALE`, `FEMENINO` | `F` |
| `M`, `MALE`, `MASCULINO` | `M` |
| `OTHER`, `OTRO`, `X` | `OTRO` |
| vacío, nulo o cualquier otro valor | `DESCONOCIDO` |

Antes de mapear se aplica `strip()` y `upper()`, así que `Female` o ` male ` también se reconocen.

## 3. Decisiones no indicadas en el enunciado

**Bronze**
- Los CSV se leen con `encoding="utf-8-sig"`: el BOM de 2024 no forma parte del nombre de la columna (sin esto, la primera columna se llamaría `﻿id_recorrido`).
- La columna sin nombre del CSV 2019 queda como `Unnamed: 0`, que es el nombre que le asigna pandas. Se conserva porque bronze no descarta columnas.
- La escritura es por partes de 500.000 filas (`chunksize`) sobre un único `data.parquet` por año. Con ~2 GB de RAM libres no entra el año 2019 completo como texto.
- Cada archivo se escribe primero como `.tmp` y se renombra al terminar. Una ejecución cortada no deja un Parquet a medias, y una ejecución repetida reemplaza el archivo, lo que mantiene la idempotencia. Si una salida no tiene filas, se borra el archivo de la corrida anterior: un archivo faltante da un error visible, mientras que datos viejos pasarían como si fueran nuevos.

**Contrato con la fuente**
- Antes de transformar cada archivo de bronze, silver comprueba que estén todas las columnas de `MAPEO_VIAJES` y `MAPEO_USUARIOS` para ese año. Si la fuente renombra una columna, el pipeline se frena con un mensaje que dice qué columna falta y qué mapeo revisar, en lugar de un error de pyarrow difícil de leer. Bronze no se valida: es la copia fiel de lo que llegó.
- `_archivo_origen` guarda el nombre del ZIP si la fuente es un ZIP, o el del CSV si la fuente es una carpeta extraída.

**Silver: viajes**
- **id_viaje de 2016**: el archivo no trae id. Se genera `2016-<n>`, donde `n` es la posición de la fila en el archivo original. Es estable entre ejecuciones (idempotente) y no puede chocar con los ids numéricos de 2019 y 2024.
- **Sufijos en los ids**: en 2019 se quita `BAEcobici` (`5BAEcobici` → `5`) y en 2024 se quita `.0` (`992557.0` → `992557`). Sin esto, los ids de estación no se pueden convertir a número y los ids de usuario no coinciden con el archivo de usuarios.
- **Duración**: 2019 y 2024 la traen en segundos; en 2019 se quita la coma de miles. En 2016 se calcula `fin - inicio`, porque su columna de duración viene en otra unidad y además no es confiable (§4). Si la duración de origen no se puede leer, también se calcula con las fechas.
- **Nombres de estación**: además de `strip`, `upper` y colapsar espacios (lo que pide el enunciado), se quitan:
  - las tildes (`Pacífico` en 2016 y `Pacifico` en 2019 quedan los dos `PACIFICO`);
  - el prefijo numérico `NNN - ` de 2019 y 2024 (`005 - Plaza Italia` → `PLAZA ITALIA`), así un mismo nombre se puede comparar entre años.
- **Duplicados**: la regla DUPLICADO se evalúa sobre todo el histórico (2016 → 2019 → 2024) y no solo dentro de cada año. Como se procesa por partes, se guarda un hash de 64 bits de cada `id_viaje` ya visto. La probabilidad de colisión con unos 10 millones de ids es de ~1e-5. Una fila cuenta como duplicada si su id ya apareció antes, aunque esa primera aparición haya ido a cuarentena.
- Las columnas de origen que no están en el esquema canónico (coordenadas, dirección, modelo de bicicleta, género del viaje) se descartan en silver. Siguen disponibles en bronze.

**Silver: usuarios**
- **Edad**: las edades menores a 16 o mayores a 90 se reemplazan por nulo y no se rechaza el registro (140 casos, por ejemplo 0, 935 o 2021). Son errores de carga evidentes, pero el resto de los datos del usuario sigue siendo útil.
- **Cuarentena de usuarios**: los usuarios rechazados (motivo `ID_NULO` o `DUPLICADO`) van a `silver/usuarios_cuarentena.parquet`, siguiendo el mismo criterio que con los viajes: no se borra nada.
- **Duplicados**: 5.231 usuarios dados de alta en 2016 vuelven a aparecer en el archivo de 2024. Queda el registro con `fecha_alta` más reciente, como pide el enunciado. Los registros de 2016 van a cuarentena.

**Gold**
- `dim_fecha` cubre todos los días entre el primer y el último viaje (2016-01-01 a 2024-12-31), incluidos los años sin datos (2017, 2018 y 2020–2023). Así es un calendario continuo.
- `dim_estacion`: si en el año más reciente una estación tuvo más de un nombre, se toma el más usado.
- **Un mismo id de estación no siempre es la misma estación.** De los 67 ids que aparecen en 2016 y en 2024, 15 tienen otro nombre. Algunos son renombres (`ZOOLOGICO` → `ECOPARQUE`) y otros son estaciones distintas que reutilizan el id (`URQUIZA` → `PARQUE PATRICIOS II`, `PERON` → `COLONIA EXPRESS`). Si se usa solo el nombre más reciente, se reescribe la historia: es una dimensión de cambio lento tipo 1. Por eso gold también guarda `estacion_nombre_anio.parquet`, con el nombre más usado de cada id en cada año (en la práctica, un SCD tipo 2 con vigencia anual). El reporte 2 usa el nombre que la estación tenía ese año.
- `fact_viajes.es_circular` compara ids de estación, no nombres.
- Reporte 3: los viajes sin usuario o con un usuario que no está en `dim_usuario` se informan como `DESCONOCIDO / DESCONOCIDO` en lugar de descartarse.
- Reporte 2: ante un empate en cantidad de viajes, ordena por `id_estacion`, así el resultado no depende del orden de las filas. Siempre hay exactamente 10 estaciones por año. Con los datos actuales no hay empates.

## 4. Hallazgos de calidad y verificaciones

### Conciliación silver (filas bronze = filas silver + filas cuarentena)

| Año | Bronze | Silver | Cuarentena | % | Motivos |
|---|---|---|---|---|---|
| 2016 | 596.807 | 503.252 | 93.555 | 15,68 | FECHA_INVALIDA 93.555 |
| 2019 | 6.217.004 | 6.207.851 | 9.153 | 0,15 | DURACION_MAYOR_24H 7.393 · ESTACION_NULA 1.760 |
| 2024 | 3.559.284 | 3.555.217 | 4.067 | 0,11 | FECHA_INVALIDA 3.379 · DURACION_MAYOR_24H 687 · DUPLICADO 1 |

Usuarios: 320.158 en bronze = 314.927 en silver + 5.231 en cuarentena (todos DUPLICADO).

- **2016, FECHA_INVALIDA**: las últimas 93.555 filas del archivo (desde el 27/06/2016) no tienen fecha de destino. Se evaluó reconstruirla como `inicio + duracion_recorrido`, pero en las filas que sí tienen las dos fechas la duración informada coincide con `fin - inicio` (±60 s) en solo el 3,7 % de los casos. La diferencia mediana es de unos 3 minutos y llega a casi 3 horas. Esa columna no es confiable, así que las filas quedan en cuarentena y no se inventan fechas. Por el mismo motivo, en 2016 la duración siempre se calcula con las fechas.
- **2024, FECHA_INVALIDA**: son viajes sin fecha de destino, sobre todo de fines de diciembre de 2024. Probablemente son viajes que todavía no habían terminado cuando se exportó el archivo.
- **Nombres de estación raros en 2019**: los ids 1 y 10 se llaman `ERRONEA` y `KP1808000368` en 2019. En 2016 esos mismos ids eran `FACULTAD DE DERECHO` y `PUERTO MADERO - UCA`. En una versión anterior, el top 10 de 2016 mostraba los nombres de 2019 porque tomaba el nombre más reciente (ver Gold, §3). Los nombres de 2019 vienen así en el origen y se conservan: no hay un criterio documentado para corregirlos.
- `id_viaje` es único en `silver/viajes`, `id_usuario` es único en `silver/usuarios` y los dtypes coinciden con el esquema canónico. `validar_esquema` se ejecuta antes de cada escritura.
- **Primero se verifica y después se publica.** Las verificaciones de silver (conciliación y unicidad) y las de gold (filas de `fact_viajes` y claves en las dimensiones) se hacen antes de reemplazar los archivos. Mientras se escribe, los Parquet grandes quedan como `.tmp`. Si una verificación falla, el `.tmp` se descarta y la capa queda entera en su versión anterior: no se publican datos a medias que después se descubre que estaban rotos. Para comprobarlo se forzó un conteo imposible en silver y en gold: las dos etapas fallaron y ningún archivo publicado cambió.
- **Umbral de cuarentena** (`UMBRAL_CUARENTENA_VIAJES` y `UMBRAL_CUARENTENA_USUARIOS` en `src/silver.py`). La conciliación sola no alcanza: si la fuente cambia el formato de fecha, `errors="coerce"` manda todo el año a cuarentena como `FECHA_INVALIDA`, la cuenta bronze = silver + cuarentena sigue dando bien y los reportes salen sin ese año. Por eso silver se frena si la cuarentena supera el 20 % en 2016 (hoy hay 15,7 % por las fechas de destino faltantes), el 5 % en 2019 y 2024 (hoy, alrededor del 0,1 %) o el 5 % en usuarios (hoy, 1,6 %). Se probó cambiando el formato de fecha de 2016: el 100 % de las filas fue a cuarentena, el pipeline se frenó y silver no cambió.

### Gold

- `fact_viajes` tiene 10.266.320 filas, igual que `silver/viajes`. Todos los `fecha_key` existen en `dim_fecha` y todos los ids de estación existen en `dim_estacion` (las dos condiciones se comprueban con `assert` en `gold.py`).
- **Viajes sin usuario en `dim_usuario`**:
  - 503.252 viajes no tienen `id_usuario`: son todos los de 2016, porque ese archivo no lo trae;
  - 6.120.635 viajes tienen un `id_usuario` que no está en `dim_usuario`.

  **Explicación**: cada archivo de usuarios trae solo las **altas de ese año**, no el padrón completo. Un viaje de 2019 hecho por alguien que se registró en 2017 o 2018 no tiene a su usuario en ninguno de los tres archivos (solo el ~24 % de los viajes de 2019 corresponde a altas de 2019). Por eso el reporte 3 tiene un grupo `DESCONOCIDO` grande.
- Idempotencia: se ejecutó `python main.py` dos veces seguidas. La cantidad de filas no cambió y los cinco CSV de reportes salieron idénticos byte a byte.

### ¿Cambia el top 10 de estaciones entre años?

Sí, parcialmente. PACIFICO, CONGRESO y PLAZA ITALIA están en el top 10 de los tres años. CONSTITUCION es la primera en 2019 y 2024 (en 2016 no figura en el top). En 2024 entran estaciones nuevas, como PLAZA DE LA SHOA y JULIETA LANTERI, por la expansión de la red. FACULTAD DE DERECHO figura en 2016 y en 2024, pero con ids distintos (1 y 202). Además, el volumen de 2016 es mucho menor (99 estaciones de origen en los datos, contra ~400 en 2019 y 2024).
