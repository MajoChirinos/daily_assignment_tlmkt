# Daily Assignment - Telemarketing

[English](README.md) | **Español**

## Descripción
Sistema automatizado de asignación diaria de usuarios para operadores de telemarketing. El proceso actual distribuye usuarios de manera equitativa por **país** y prioriza primero los usuarios de mayor prioridad dentro de cada país.

El sistema considera:

- **Distribución por país**: Cada operador recibe uno o más países desde `lp_tlmkt_current`
- **Orden por prioridad**: Los usuarios se asignan de mayor a menor prioridad (`ULTRA-1`, `ULTRA-2`, `ALTA-1`, ...)
- **Completar con fallback**: Países de respaldo opcionales pueden completar cuotas incompletas cuando un país se queda sin usuarios
- **Filtrado base de calidad**: Excluye usuarios sin `phone` o sin un `firstLast_name` usable antes de asignar y antes de reservar grupo control
- **Exclusión de usuarios contactados**: Excluye usuarios contactados según ventanas de descarte configurables — globales o por campaña — desde `days_ago_to_discard` hasta ayer
- **Reserva de grupo control**: Permite reservar grupos control por campaña antes de la asignación de tratamiento usando `control_group_percent`
- **Filtrado por moneda y campaña**: Permite apagar una campaña en un país específico mediante `campaigns_to_filter_by_currency`
- **Comportamiento controlado por configuración**: La asignación y el filtrado se controlan desde tablas de BigQuery

## Referencias Operativas

- **Dataset de configuración**: `mi-casino.dm_telemarketing`
  - `conf_parameters_v2`: parámetros de asignación
  - `conf_segments_to_consult`: segmentos a consultar
  - `lp_tlmkt_current`: lista de operadores
  - `conf_priority_by_campaign`: no se consulta en el algoritmo actual por país; las prioridades vienen de los segmentos
- **Looker Studio**: [Dashboard de asignación de telemarketing](https://datastudio.google.com/reporting/08c9ba40-b514-4715-98d1-d8b22a7587a0/page/p_q0dsmy25zd/edit)
  - Página 1: asignación diaria por operador, ordenada por operador y prioridad para que al descargarse aparezcan primero los usuarios de mayor prioridad
  - Página 2: data diaria disponible por país y prioridad

## Evaluación del Estado Actual

- La extracción consulta las tablas de segmentos configuradas y omite tablas inexistentes o vacías sin detener toda la corrida.
- Los filtros base eliminan monedas bloqueadas (`CAD`, `ARS`, `BRL`), conservan niveles 1-3 y remueven filas sin `phone`, con `firstLast_name` nulo o con `firstLast_name` vacío.
- El DataFrame limpio `available_users` alimenta tanto el grupo control como la asignación de tratamiento; por eso un usuario descartado en extracción no puede entrar en ninguno de los dos grupos.
- Las ventanas de descarte por campaña se leen desde `conf_segments_to_consult`; la ventana más larga define cuánto historial se descarga desde BigQuery.
- Los usuarios de control se reservan antes de asignar operadores, quedan con `group_flag = CONTROL`, y los usuarios asignados quedan como `TREATMENT`.
- Actualmente `main.py` llama al cargador con `load_data=False`, así que la corrida escribe Excels locales y crea/valida tablas, pero no agrega filas a BigQuery salvo que se cambie ese flag.

## Estructura del Proyecto

```
daily_assignment_tlmkt/
├── data/                          # Archivos de salida de asignaciones (Excel)
├── src/
│   ├── config.py                  # Clase de configuración dinámica
│   ├── extract.py                 # Extracción de BigQuery
│   ├── transform.py               # Algoritmos de asignación y normalización
│   ├── load.py                    # Carga de datos a BigQuery
│   └── __pycache__/              # Archivos compilados de Python
├── .env                          # Variables de entorno (no en repo)
├── .gitignore                    # Exclusiones de control de versiones
├── main.py                       # Ejecutable principal (compatible con Cloud Run)
├── test_main.py                  # Script de pruebas locales
├── requirements.txt              # Dependencias del proyecto
├── xxx-xxxxxx-xxxx-xxxx.json # Credenciales de servicio para BigQuery (no en repo)
├── README.md                     # Documentación del proyecto (inglés)
└── README.es.md                  # Documentación del proyecto (español)
```

## Configuración del Sistema

### Clase Config
La clase `Config` gestiona dinámicamente todos los parámetros del sistema desde `mi-casino.dm_telemarketing.conf_parameters_v2`:

```python
class Config:
    """
    Gestiona parámetros de configuración desde DataFrame.
    
    Convierte automáticamente tipos de datos:
    - int: Números enteros (días, cantidades)
    - float: Números decimales (porcentajes)  
    - str: Cadenas de texto
    - list(str): Listas de strings separadas por comas
    - bool: Valores booleanos (`TRUE` / `FALSE`, `1` / `0`, `yes` / `no`)
    - dict(str,list(str)): Diccionario de moneda → lista de campañas (formato: `PEN:camp1,camp2|BOB:camp1`)
    """
```

**Parámetros principales:**
- `days_ago_to_discard`: Ventana de descarte global en días. Actúa como fallback para campañas que no tengan valor propio en `conf_segments_to_consult`. También determina cuántos días atrás se descarga el historial si no hay campañas con ventana mayor.
- `exclude_email_mkt_users`: Indica si se incluye el historial de email marketing en el descarte (`TRUE` / `FALSE`)
- `users_to_assign_per_operator`: Cantidad base de usuarios por operador (ej: 100)
- `currencies_to_filter`: Lista de monedas a excluir en la extracción
- `campaigns_to_filter`: Lista de campañas a excluir globalmente (todos los países)
- `campaigns_to_filter_by_currency`: Campañas a excluir por moneda específica. Permite apagar una campaña solo en un país sin afectar los demás. Formato: `PEN:sport_events,reactivation|BOB:sport_events`. Si la celda está vacía no se aplica ningún filtro.
- `extra_users_country`: Países de respaldo opcionales para completar cuotas incompletas

### Distribución por País del Operador

  El sistema utiliza un split proporcional basado en la cantidad de países asignados a cada operador:

```python
percentages = {
  1: [1.0],           # 100% para operadores con 1 país
  2: [0.7, 0.3],      # 70% y 30% para operadores con 2 países  
  3: [0.5, 0.3, 0.2]  # 50%, 30% y 20% para operadores con 3 países
}
```

**Lógica de asignación:**
- **1 país**: El operador recibe el 100% de su cuota en ese país
- **2 países**: El primer país recibe 70%, el segundo 30%
- **3 países**: Distribución 50%-30%-20% en el orden configurado

**Ejemplo práctico:**
Si un operador debe recibir 100 usuarios y maneja 3 países:
- País 1: 50 usuarios (50%)
- País 2: 30 usuarios (30%)  
- País 3: 20 usuarios (20%)

## Instalación y Configuración

### Prerrequisitos
```bash
# Python 3.8+
# Google Cloud CLI configurado
# Credenciales de BigQuery
```

### Instalación de dependencias
```bash
pip install -r requirements.txt
```

### Autenticación
```bash
# Usar credenciales CLI (recomendado para desarrollo)
gcloud auth application-default login

# O configurar variable de entorno para producción
export GOOGLE_APPLICATION_CREDENTIALS="ruta/a/service-account.json"
```

### Credenciales para Cloud Run
Usar la cuenta de servicio de Cloud Run mediante Application Default Credentials. Debe poder ejecutar consultas de BigQuery y leer las tablas de configuración, operadores, segmentos e historial.

Algunas tablas de configuración son **tablas externas de BigQuery respaldadas por Google Sheets** (por ejemplo, `conf_parameters_v2`). BigQuery lee el Sheet con las credenciales de quien ejecuta la consulta, por lo que la cuenta de servicio de Cloud Run debe tener acceso de lector a cada Google Sheet asociado (compartirlo con el correo de la cuenta de servicio). Sin ese acceso, las consultas a esas tablas fallan aunque la cuenta tenga permisos sobre el dataset. El código solicita el scope `drive.readonly` para este fin.

## Uso

### Pruebas Locales (test_main.py)
El script `test_main.py` te permite probar el código de `main.py` listo para Cloud Run localmente, simulando el entorno de Cloud Run:

```bash
# test_main.py es un runner local basado en test_main.example.py.
# Lee los nombres de credenciales locales desde local_credentials_config.json.
# local_credentials_config.json está ignorado por Git; usar local_credentials_config.example.json como plantilla.

python test_main.py
```

Configuración local esperada:

```json
{
  "bigquery_credentials_file": "ruta/a/bigquery-service-account.json"
}
```

**Propósito**: Probar el código exacto que se ejecutará en Cloud Run sin desplegarlo. Útil para:
- Validar la lógica de asignación antes del despliegue
- Depurar problemas de credenciales localmente
- Probar cambios de configuración
- Verificar extracción y transformación de datos

**Salida**: Log completo de ejecución con el estado del resultado final.

### Despliegue en Cloud Run
```bash
# Desplegar a Cloud Run
gcloud run deploy daily-assignment-tlmkt \
  --source . \
  --region southamerica-west1 \
  --allow-unauthenticated

# Dar acceso a las tablas de BigQuery a la cuenta de servicio de Cloud Run.
```

**Punto de entrada**: función `run_daily_assignment(request)` en main.py

## Mejoras Recientes (2026)

### Manejo Mejorado de Errores
- **Manejo de errores por tabla individual**: Si una tabla de BigQuery no existe o está vacía, el proceso continúa con las tablas restantes
- **Bloques try-except completos**: Todas las operaciones críticas envueltas con manejo de errores
- **Mensajes de error informativos**: Registro claro para depuración en Cloud Run

### Seguridad en Carga de Datos
- **Parámetro `delete_today`**: Control sobre si reemplazar o prevenir datos duplicados
  - `True`: Reemplaza los datos de hoy si existen
  - `False`: Previene la carga si ya existen datos para hoy (recomendado para producción)
- **Verificación inteligente de eliminación**: Verifica si existen datos antes de intentar eliminarlos
- **Mensajes de estado claros**: Los logs muestran exactamente qué pasó con los datos

### Características Adicionales
- **Columna `campaign_details`**: Soporte para metadatos de campañas externas
- **Columna `group_flag`**: Identifica filas `CONTROL` y `TREATMENT` en la salida diaria
- **Metadata de experimento**: Agrega `experiment_id`, `channel` y `user_campaign_id` determinístico
- **Seguimiento de descarga de tablas**: Muestra qué tablas se están descargando (* nombre_tabla)
- **Detección de tablas vacías**: Advierte cuando las tablas existen pero no contienen datos (⚠️)
- **Validación de nombre**: Las filas sin `firstLast_name` se eliminan antes de cualquier lógica de asignación

## Flujo del Proceso

### 1. **Configuración y Credenciales**
- Carga configuración desde BigQuery usando la clase `Config`
- Establece Application Default Credentials para BigQuery
- Define fechas y parámetros de filtrado

### 2. **Extracción de Datos (Extract)**
- **Operadores activos**: Lista desde la tabla de BigQuery `lp_tlmkt_current`
- **Usuarios disponibles**: Segmentos de BigQuery según configuración
- **Filtros base**: Eliminación de monedas bloqueadas, niveles no soportados, teléfonos faltantes y `firstLast_name` nulo o vacío
- **Historial de asignaciones**: Usuarios contactados recientemente por telemarketing (`tlmkt_DailyAssignment`) y email marketing (`email_mkt_DailyAssignment`)
- **Configuración de campañas**: Parámetros dinámicos del sistema

### 3. **Transformación y Asignación (Transform)**
- **Filtrado de usuarios**: Exclusión de usuarios contactados recientemente por telemarketing o email marketing, con ventana de descarte configurable por campaña
- **Filtrado por moneda y campaña**: Exclusión de campañas específicas para monedas/países concretos (`campaigns_to_filter_by_currency`)
- **Normalización de campañas**: Conversión entre códigos internos y nombres en español
- **Creación de DataFrames por campaña**: Organización de usuarios disponibles para métricas y reportes
- **Reserva de grupo control**: Usuarios de control por campaña se separan desde el pool limpio antes de asignar tratamiento
- **Asignación por cuota de país**: Los operadores reciben cuotas por país desde `lp_tlmkt_current`
- **Asignación 1 a 1**: Los usuarios se asignan uno por uno por operador para mantener equilibrio
- **Prioridad primero**: Los usuarios de mayor prioridad se asignan primero dentro de cada país
- **Países de fallback**: `extra_users_country` puede completar cuotas incompletas si está configurado

### 4. **Carga de Datos (Load)**
- **Archivo local**: Excel con asignaciones del día
- **BigQuery**: Tabla de asignaciones históricas
- **Marcado de grupo**: La salida diaria incluye `CONTROL` y `TREATMENT` mediante `group_flag`
- **Flag de carga**: El append a BigQuery depende de `load_data`; el código actual pasa `load_data=False`
- **Normalización final**: Conversión de códigos a nombres en español

### Tablas de BigQuery

#### `tlmkt_DailyAssignment`

Esquema actual usado por el cargador:

| campo | tipo |
|-------|------|
| assignment_date | DATETIME |
| operator | STRING |
| campaign_name | STRING |
| user_id | INTEGER |
| username | STRING |
| firstLast_name | STRING |
| phone | STRING |
| level | INTEGER |
| register_currency | STRING |
| last_activity | DATETIME |
| campaign_details | STRING |
| priority | STRING |
| op_email | STRING |
| group_flag | STRING |
| experiment_id | STRING |
| channel | STRING |
| user_campaign_id | STRING |

#### `tlmkt_AssignmentMetrics`

Esquema actual usado por el cargador:

| campo | tipo |
|-------|------|
| assignment_date | DATETIME |
| country | STRING |
| priority | STRING |
| campaign | STRING |
| available_users | INTEGER |
| assigned_users | INTEGER |
| unassigned_users | INTEGER |

## Archivos de Configuración

### Fuentes de BigQuery requeridas:
Todas las tablas están en `mi-casino.dm_telemarketing`:
1. **conf_parameters_v2**: Parámetros del sistema (`variable`, `value`, `type`)
2. **conf_segments_to_consult**: Tablas de segmentos y controles por campaña
3. **lp_tlmkt_current**: Lista de operadores con campos en inglés

`conf_priority_by_campaign` no se consume en el algoritmo actual. La asignación usa los valores de `priority` de las tablas de segmentos.

Las tablas de configuración (como `conf_parameters_v2`) pueden ser tablas externas sobre Google Sheets. Para editar sus valores, modifica el Sheet de origen (revisa en BigQuery la sección "Configuración de datos externos" de cada tabla para ver la URI).

### Estructura de configuración:
| variable | value | type |
|----------|-------|------|
| days_ago_to_discard | 7 | int |
| exclude_email_mkt_users | FALSE | bool |
| users_to_assign_per_operator | 100 | int |
| currencies_to_filter | BOB | list(str) |
| campaigns_to_filter | reactivation | list(str) |
| campaigns_to_filter_by_currency | PEN:sport_events\|BOB:sport_events | dict(str,list(str)) |
| extra_users_country | VES | list(str) |

### Estructura de conf_segments_to_consult:
| table_name | control_group_percent | description | campaign_label | days_ago_to_discard |
|---|---|---|---|---|
| tlmkt_Sport_Events | 0 | Eventos Deportivos | sport_events | 7 |
| tlmkt_Non_Depositors | 0 | No Depositantes | non_depositors | 21 |

`days_ago_to_discard` en `conf_segments_to_consult` define la ventana de descarte por campaña. Si es nulo o vacío, usa el valor global del parámetro homónimo en `conf_parameters_v2`.

`control_group_percent` define el porcentaje del pool de cada campaña que se reserva como control antes de la asignación de tratamiento. Usar `0` o dejarlo vacío cuando no se requiera grupo control.

## Mantenimiento y Administración

### Actualizar Operadores
Editar la tabla de BigQuery `mi-casino.dm_telemarketing.lp_tlmkt_current`:
- **full_name**: Nombre completo, convertido internamente a `operator`
- **dotpanel_username**: Username del sistema, convertido a `user_dotpanel`
- **corporate_email**: Correo, convertido a `op_email`; nulo o vacío significa sin correo
- **country**: Países separados por comas, conservando su orden para las cuotas
- **job_title**: "Ejecutivo de Televentas"
- **status**: "Activo" para incluir en asignaciones
- **shift**, **campaign**, **notion_permission_type**: Campos presentes en la LP, no usados en la asignación actual

### Modificar Parámetros del Sistema
Editar la tabla de BigQuery `mi-casino.dm_telemarketing.conf_parameters_v2`:
- Cambiar monedas y campañas excluidas
- Ajustar días de exclusión
- Modificar cantidad de usuarios por operador
- Activar o desactivar la exclusión de email marketing
- Definir países de fallback para completar cuotas incompletas

### Troubleshooting Común
- **Error de credenciales**: Verificar `gcloud auth list`
- **Datos faltantes**: Revisar tablas de configuración de BigQuery y permisos de acceso
- **Usuarios sin nombre**: Confirmar que las tablas fuente llenen `firstLast_name`; la extracción elimina nombres nulos y vacíos antes de asignar
- **Asignaciones desbalanceadas**: Revisar el mapeo de países en LP, los países fallback y la distribución por prioridad
- **Desfase de esquema**: Verificar que las tablas de BQ incluyan `country` y `priority` en `tlmkt_AssignmentMetrics`
- **No carga en BQ**: Confirmar que `load_data=True` en `main.py`
- **Tablas no encontradas**: Verificar nombres de tablas en BigQuery

## Ejemplos de Salida

### Logs de Ejecución
```
History fetch window: 21 days back (2026-06-19)
Per-campaign discard windows: {'sport_events': 7, 'non_depositors': 21, 'reactivation': 14}
Filtering campaigns by currency:
  - PEN: ['sport_events']
  [PEN] Removed 342 users from campaigns ['sport_events']
Users after currency-campaign filter: 15219
Discarding previously contacted users (per-campaign windows, up to 2026-07-09)
Available users for assignment: 15561
```

## Licencia
Este proyecto es de uso interno de la organización.
